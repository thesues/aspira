// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raftwal

import (
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"
	"github.com/thesues/aspira/protos/aspirapb"
	"github.com/thesues/aspira/utils"
	"github.com/thesues/aspira/xlog"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/lump"
	cannyls "github.com/thesues/cannyls-go/storage"
)

type WAL struct {
	p          unsafe.Pointer //the type is *cannyls.Storage
	ab         *block.AlignedBytes
	sab        *block.AlignedBytes
	dbLock     *sync.Mutex //protect readCounts
	readCounts int
	// Protects access to all fields. Most methods of MemoryStorage are
	sync.Mutex
	snapshot raftpb.Snapshot
	// ents[i] has raft log position i+snapshot.Metadata.Index
	ents       []aspirapb.EntryMeta //only cache EntryMeta
	entryCache *lru.Cache           //full cache contains user data
	lastWrite  int64                //seconds
}

var (
	keyMask                = (^uint64(0) >> 2) //0x3FFFFFFFFFFFFFFF, the first two bits are zero
	minimalKey             = ^keyMask
	maxKey                 = keyMask
	endOfList              = errors.Errorf("end of list of keys")
	errNotFound            = errors.New("Unable to find raft entry")
	throttle               = (40 << 10)
	snapshotCatchUpEntries = uint64(3000)
)

func Init(db *cannyls.Storage) *WAL {

	entryCache, err := lru.New(4)
	if err != nil {
		xlog.Logger.Errorf("can not create LRU %+v", err)
		return nil
	}
	wal := &WAL{
		ab:         block.NewAlignedBytes(512, block.Min()),
		sab:        block.NewAlignedBytes(512, block.Min()),
		readCounts: 0,
		dbLock:     new(sync.Mutex),
		entryCache: entryCache,
		ents:       make([]aspirapb.EntryMeta, 1, 2000),
		snapshot:   raftpb.Snapshot{},
		lastWrite:  time.Now().Unix(),
	}
	atomic.StorePointer(&wal.p, unsafe.Pointer(db))

	//if db is silent, run SideJob

	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if time.Now().Unix()-atomic.LoadInt64(&wal.lastWrite) > 3 {
					db := wal.DB()
					if db != nil {
						db.RunSideJobOnce(128)
					}
				}
			}

		}
	}()

	return wal
}

func (wal *WAL) snapshotKey() (ret lump.LumpId) {
	//startsWith 0b10XXXXX
	//0x80 + 7 (byte)
	var buf [8]byte
	buf[0] = 0x80
	copy(buf[1:], []byte("snapKey"))
	ret, err := lump.FromBytes(buf[:])
	if err != nil {
		panic("snapshotKey failed")
	}
	return
}

func (wal *WAL) hardStateKey() (ret lump.LumpId) {
	//startsWith 0b10XXXXX
	//0x80 + 7 (byte)
	var buf [8]byte
	buf[0] = 0x80
	copy(buf[1:], []byte("hardKey"))
	ret, err := lump.FromBytes(buf[:])
	if err != nil {
		panic("snapshotKey failed")
	}
	return
}

func (wal *WAL) Save(hd raftpb.HardState, entries []raftpb.Entry) (err error) {

	if err = wal.setHardState(hd); err != nil {
		return err
	}
	if err = wal.addEntries(entries); err != nil {
	}
	return nil
}

//reset for test only
func (wal *WAL) reset(entries []raftpb.Entry) {
	wal.ents = nil
	wal.deleteFrom(0)
	wal.entryCache.Purge()
	for _, e := range entries {
		var m aspirapb.EntryMeta
		switch e.Type {
		case raftpb.EntryConfChange:
			m = aspirapb.EntryMeta{Index: e.Index, Term: e.Term, EntryType: aspirapb.EntryMeta_ConfChange, Data: e.Data}
		case raftpb.EntryNormal:
			m = aspirapb.EntryMeta{Index: e.Index, Term: e.Term, EntryType: aspirapb.EntryMeta_NormalOther, Data: e.Data}
		}
		wal.ents = append(wal.ents, m)
		data, _ := m.Marshal()
		wal.DB().PutEmbed(wal.EntryKey(e.Index), data)
	}
}

func (wal *WAL) Sync() {
	wal.DB().Sync()
}

func (wal *WAL) Flush() {
	wal.DB().Flush()
}

func (wal *WAL) HardState() (raftpb.HardState, error) {
	return wal.hardState()
}

func (wal *WAL) hardState() (hd raftpb.HardState, err error) {
	data, err := wal.DB().Get(wal.hardStateKey())
	//if err is noSuchKey, but Unmarshal will still success, and return a nil error
	err = hd.Unmarshal(data)
	return
}

func (wal *WAL) setHardState(st raftpb.HardState) (err error) {
	if raft.IsEmptyHardState(st) {
		return nil
	}
	data, err := st.Marshal()
	if err != nil {
		return errors.Wrapf(err, "wal.Store: While marshal hardstate")
	}

	_, err = wal.DB().PutEmbed(wal.hardStateKey(), data)
	atomic.StoreInt64(&wal.lastWrite, time.Now().Unix())

	return
}

func (wal *WAL) Snapshot() (snap raftpb.Snapshot, err error) {
	wal.Lock()
	defer wal.Unlock()
	return wal.snapshot, nil
}

func (wal *WAL) ExtKey(idx uint64) lump.LumpId {
	if idx > keyMask {
		panic("idx is too big")
	}
	idx |= 1 << 62 //first two bit "01"
	return lump.FromU64(0, idx)

}

// EntryKey returns the key where the entry with the given ID is stored.
func (wal *WAL) EntryKey(idx uint64) lump.LumpId {
	if idx > keyMask {
		panic("idx is too big")
	}
	idx |= 1 << 63 //first two bit "11"
	idx |= 1 << 62
	return lump.FromU64(0, idx)
}

// LastIndex implements the Storage interface.
func (wal *WAL) LastIndex() (uint64, error) {
	wal.Lock()
	defer wal.Unlock()
	return wal.lastIndex(), nil
}

func (wal *WAL) lastIndex() uint64 {
	return wal.ents[0].Index + uint64(len(wal.ents)) - 1
}

// FirstIndex implements the Storage interface.
func (wal *WAL) FirstIndex() (uint64, error) {
	wal.Lock()
	defer wal.Unlock()
	return wal.firstIndex(), nil
}

func (wal *WAL) firstIndex() uint64 {
	return wal.ents[0].Index + 1
}

func (wal *WAL) addEntries(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	wal.Lock()
	defer wal.Unlock()

	first := wal.firstIndex()
	last := entries[0].Index + uint64(len(entries)) - 1
	// shortcut if there is no new entry.
	if last < first {
		return nil
	}

	// truncate compacted entries
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}

	offset := entries[0].Index - wal.ents[0].Index

	if uint64(len(wal.ents)) > offset {
		//have overlap with current log, truncate current log
		wal.ents = append([]aspirapb.EntryMeta{}, wal.ents[:offset]...)
		//wal.DeleteFrom(entries[0].Index)
	} else if uint64(len(wal.ents)) == offset {
		//normal append
	} else {
		xlog.Logger.Panicf("missing log entry [last: %d, append at: %d]",
			wal.lastIndex(), entries[0].Index)
	}

	for _, e := range entries {
		var entryMeta aspirapb.EntryMeta
		entryMeta.Term = e.Term
		entryMeta.Index = e.Index
		//bigger than 50KB
		switch e.Type {
		case raftpb.EntryConfChange:
			entryMeta.EntryType = aspirapb.EntryMeta_ConfChange
		case raftpb.EntryNormal:
			if len(e.Data) > throttle {
				var proposal aspirapb.AspiraProposal
				err := proposal.Unmarshal(e.Data)
				utils.Check(err)
				if proposal.ProposalType == aspirapb.AspiraProposal_Put {
					entryMeta.EntryType = aspirapb.EntryMeta_NormalPutBig
					entryMeta.AssociateKey = proposal.AssociateKey
					wal.ab.Resize(uint32(len(proposal.Data)))
					copy(wal.ab.AsBytes(), proposal.Data)
					xlog.Logger.Debugf("Wrote Ext data %x\n\n", wal.ExtKey(entryMeta.Index).U64())
					_, err := wal.DB().Put(wal.ExtKey(entryMeta.Index), lump.NewLumpDataWithAb(wal.ab))
					if err != nil {
						return err
					}
					xlog.Logger.Debugf("cached %d", e.Index)
					wal.entryCache.Add(e.Index, e)
				} else {
					panic("must be AspiraProposal_Put")
				}
			} else {
				entryMeta.EntryType = aspirapb.EntryMeta_NormalOther
				entryMeta.Data = e.Data
			}
		default:
			xlog.Logger.Fatal("meet new raftpb type")
		}

		data, err := entryMeta.Marshal()
		if err != nil {
			return err
		}
		if _, err = wal.DB().PutEmbed(wal.EntryKey(entryMeta.Index), data); err != nil {
			return err
		}
		wal.ents = append(wal.ents, entryMeta)
	}
	return nil
}

// Delete entries in the range of index [from, inf).
// both LOG and extendLOG will be removed
func (wal *WAL) deleteFrom(from uint64) error {
	logStart := wal.EntryKey(from)
	logEnd := lump.FromU64(0, ^uint64(0)) //0xFFFFFFFFFFFFFFFF

	exLogStart := wal.ExtKey(from)
	exLogEnd := lump.FromU64(0, (uint64(1)<<63)-1) //0x7FFFFFFFFFFFFFFF

	//remove LOG
	if err := wal.DB().DeleteRange(logStart, logEnd, false); err != nil {
		return err
	}

	if err := wal.DB().DeleteRange(exLogStart, exLogEnd, true); err != nil {
		return err
	}
	return nil
}

func (wal *WAL) InitialState() (hs raftpb.HardState, cs raftpb.ConfState, err error) {
	hs, err = wal.hardState()
	if err != nil {
		return
	}
	snap, _ := wal.Snapshot()
	return hs, snap.Metadata.ConfState, nil
}

//loadEntries read EntryMeta from index. Append into wal.ents, and
//then array index starts from 1
func (wal *WAL) loadEntries(index uint64) (bool, error) {
	var err error
	var data []byte
	id, hasData := wal.DB().MaxId()
	if !hasData {
		return false, nil
	}
	lo := index
	hi := (id.U64() & keyMask) + 1
	//debug
	//data range [lo, hi)
	for _, id := range wal.DB().ListRange(wal.EntryKey(lo), wal.EntryKey(hi), math.MaxUint64) {
		if data, err = wal.DB().Get(id); err != nil {
			return false, err
		}
		var meta aspirapb.EntryMeta
		if err = meta.Unmarshal(data); err != nil {
			return false, err
		}
		wal.ents = append(wal.ents, meta)
	}
	return true, nil
}

//PastLife load entries into memory and return if it is a RESTART
func (wal *WAL) PastLife() (bool, error) {
	xlog.Logger.Info("replaying WAL")

	//xlog.Logger.Info("run JournalGC")
	//wal.DB().JournalGC()
	//xlog.Logger.Info("end run JournalGC")

	data, err := wal.DB().Get(wal.snapshotKey())
	if err != nil {
		//nosnapshot so far
		return wal.loadEntries(1)
	}

	var snap raftpb.Snapshot
	err = snap.Unmarshal(data)
	if err != nil {
		return false, err
	}

	if raft.IsEmptySnap(snap) {
		return wal.loadEntries(1)
	}

	wal.snapshot = snap
	wal.ents[0].Index = snap.Metadata.Index
	wal.ents[0].Term = snap.Metadata.Term

	return wal.loadEntries(snap.Metadata.Index + 1)
}

func (wal *WAL) fromMetaToRaftEntry(meta aspirapb.EntryMeta) (e raftpb.Entry) {

	switch meta.EntryType {
	case aspirapb.EntryMeta_ConfChange:
		e.Type = raftpb.EntryConfChange
		e.Data = meta.Data
	case aspirapb.EntryMeta_NormalOther:
		e.Type = raftpb.EntryNormal
		e.Data = meta.Data
	case aspirapb.EntryMeta_NormalPutBig:

		if v, ok := wal.entryCache.Get(meta.Index); ok {
			e = v.(raftpb.Entry)
			xlog.Logger.Debugf("cache hit for %d", meta.Index)
			return e
		}

		//build data
		e.Type = raftpb.EntryNormal

		extData, err := wal.DB().Get(wal.ExtKey(meta.Index))
		if err != nil {
			xlog.Logger.Warnf("Get extdata failed %+v", err)
			break
		}
		//restore the proposal data from EntryMeta
		var proposal aspirapb.AspiraProposal
		proposal.AssociateKey = meta.AssociateKey
		proposal.Data = extData
		data, err := proposal.Marshal()
		utils.Check(err)
		e.Data = data
	default:
		xlog.Logger.Fatalf("unknow type read from %+v", meta)
	}
	e.Term = meta.Term
	e.Index = meta.Index
	return
}

func (wal *WAL) AllEntries(lo, hi, maxSize uint64) (es []raftpb.Entry, err error) {

	wal.Lock()
	defer wal.Unlock()
	offset := wal.ents[0].Index
	if lo <= offset {
		return nil, raft.ErrCompacted
	}
	if hi > wal.lastIndex()+1 {
		xlog.Logger.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, wal.lastIndex())
	}
	// only contains dummy entries.
	if len(wal.ents) == 1 {
		return nil, raft.ErrUnavailable
	}
	ents := wal.ents[lo-offset : hi-offset]
	size := 0
	var e raftpb.Entry
	for i := range ents {
		//id := ents[i].Index

		e = wal.fromMetaToRaftEntry(ents[i])
		/*
			if v, ok := wal.entryCache.Get(id); ok {
				e = v.(raftpb.Entry)
				xlog.Logger.Debugf("cache hit for %d", id)
			} else {
				e = wal.fromMetaToRaftEntry(ents[i])
			}
		*/
		es = append(es, e)
		size += e.Size()
		//if maxSize is 0, we still want to return at lease one entry
		if uint64(size) > maxSize {
			n := len(es)
			if n > 1 {
				es = es[:n-1]
			}
			break
		}
	}
	return es, nil
}

func (wal *WAL) Entries(lo, hi, maxSize uint64) (es []raftpb.Entry, err error) {

	if maxSize > (128 << 20) {
		maxSize = (128 << 20)
	}
	xlog.Logger.Debugf("search entries %d=>%d\n", lo, hi)
	first, err := wal.FirstIndex()
	if err != nil {
		return es, err
	}
	if lo < first {
		return nil, raft.ErrCompacted
	}

	last, err := wal.LastIndex()
	if err != nil {
		return es, err

	}
	if hi > last+1 {
		return nil, raft.ErrUnavailable
	}
	return wal.AllEntries(lo, hi, maxSize)
}

/*
func (wal *WAL) reset(es []raftpb.Entry) error {
	wal.DeleteFrom(0)
	wal.addEntries(es, false)
	return nil
}
*/

func (wal *WAL) deleteUntil(until uint64) error {
	//TODO delete LRU??
	if err := wal.DB().DeleteRange(wal.EntryKey(0), wal.EntryKey(until), true); err != nil {
		return err
	}
	if err := wal.DB().DeleteRange(wal.ExtKey(0), wal.ExtKey(until), true); err != nil {
		return err
	}
	return nil

}

// Term implements the Storage interface.
func (wal *WAL) Term(i uint64) (uint64, error) {
	wal.Lock()
	defer wal.Unlock()
	offset := wal.ents[0].Index
	if i < offset {
		return 0, raft.ErrCompacted
	}
	if int(i-offset) >= len(wal.ents) {
		return 0, raft.ErrUnavailable
	}
	return wal.ents[i-offset].Term, nil
}

func (wal *WAL) ApplySnapshot(snap raftpb.Snapshot) {
	wal.Lock()
	defer wal.Unlock()
	if raft.IsEmptySnap(snap) {
		return
	}
	wal.snapshot = snap
	wal.ents = []aspirapb.EntryMeta{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	//save to disk.

	//wal.DB().JournalGC()
	wal.deleteFrom(snap.Metadata.Index)
	//set snapshot key
	data, _ := wal.snapshot.Marshal()
	wal.DB().PutEmbed(wal.snapshotKey(), data)
}

//CreateSnapshot, if createSnapshot
// is done, it means we have synced the database, so the raft worker do not have to sync again
func (wal *WAL) CreateSnapshot(snapi uint64, cs *raftpb.ConfState, udata []byte) (created bool, err error) {

	wal.Lock()
	defer wal.Unlock()
	if snapi <= wal.snapshot.Metadata.Index {
		return false, raft.ErrSnapOutOfDate
	}

	offset := wal.ents[0].Index
	if snapi > wal.lastIndex() {
		return false, errors.Errorf("snapshot %d is out of bound lastindex(%d)", snapi, wal.lastIndex())
	}

	wal.snapshot.Metadata.Index = snapi
	wal.snapshot.Metadata.Term = wal.ents[snapi-offset].Term
	if cs != nil {
		wal.snapshot.Metadata.ConfState = *cs
	}
	wal.snapshot.Data = udata

	//set snapshot key
	data, err := wal.snapshot.Marshal()
	if err != nil {
		return false, errors.Errorf("can not mashal snapshot data")
	}

	wal.DB().Sync()
	if _, err = wal.DB().PutEmbed(wal.snapshotKey(), data); err != nil {
		return false, err
	}

	if wal.InflightSnapshot() {
		return true, nil
	}

	compactIndex := uint64(1)
	if snapi > snapshotCatchUpEntries {
		compactIndex = snapi - snapshotCatchUpEntries
	}

	if compactIndex <= offset {
		return
	}

	xlog.Logger.Infof("snapshot compact log at [%d]", compactIndex)
	//compact ents and cannyls
	i := compactIndex - offset
	ents := make([]aspirapb.EntryMeta, 1, 1+uint64(len(wal.ents))-i)
	ents[0].Index = wal.ents[i].Index
	ents[0].Term = wal.ents[i].Term
	ents = append(ents, wal.ents[i+1:]...)
	wal.ents = ents

	//disk
	if err = wal.deleteUntil(compactIndex + 1); err != nil {
		return
	}
	return true, nil
}

func (wal *WAL) ApplyPut(e raftpb.Entry) error {
	if len(e.Data) <= 0 {
		return errors.Errorf("data len is 0")
	}
	if len(e.Data) > throttle {
		dataPortion, err := wal.DB().GetRecord(wal.ExtKey(e.Index))
		if err != nil {
			return err
		}
		return wal.DB().WriteRecord(lump.FromU64(0, e.Index), *dataPortion)
	}

	var p aspirapb.AspiraProposal
	err := p.Unmarshal(e.Data)
	if err != nil {
		return err
	}
	wal.sab.Resize(uint32(len(p.Data)))
	copy(wal.sab.AsBytes(), p.Data)
	_, err = wal.DB().Put(lump.FromU64(0, e.Index), lump.NewLumpDataWithAb(wal.sab))
	return err
}

func (wal *WAL) ApplyPutWithOffset(index uint64) error {
	return nil
}

func (wal *WAL) Delete(key uint64) (err error) {
	_, _, err = wal.DB().Delete(lump.FromU64(0, key))
	return
}

func (wal *WAL) ObjectMaxSize() int64 {
	return int64(lump.LUMP_MAX_SIZE)
}

func (wal *WAL) GetData(index uint64) ([]byte, error) {
	if index > keyMask {
		return nil, errors.Errorf("index is too big:%d", index)
	}
	return wal.DB().Get(lump.FromU64(0, index))
}

func (wal *WAL) InflightSnapshot() bool {
	wal.dbLock.Lock()
	wal.dbLock.Unlock()
	return wal.readCounts > 0

}
func (wal *WAL) GetStreamReader() (io.Reader, error) {
	wal.dbLock.Lock()
	defer wal.dbLock.Unlock()

	reader, err := wal.DB().GetSnapshotReader()
	if err != nil {
		return nil, err
	}
	wal.readCounts++
	return reader, nil
}

func (wal *WAL) FreeStreamReader() {
	wal.dbLock.Lock()
	defer wal.dbLock.Unlock()

	if wal.readCounts == 0 {
		xlog.Logger.Fatalf("FreeStreamReader > GetStreamReader")
	}
	wal.readCounts--
	if wal.readCounts == 0 {
		if err := wal.DB().DeleteSnapshot(); err != nil {
			xlog.Logger.Errorf("failed to delete snapshot file %+v", err)
		}
	}
}

func (wal *WAL) DB() (db *cannyls.Storage) {
	v := atomic.LoadPointer(&wal.p)
	return (*cannyls.Storage)(v)
}
func (wal *WAL) SetDB(db *cannyls.Storage) {
	atomic.StorePointer(&wal.p, unsafe.Pointer(db))
}

func (wal *WAL) CloseDB() {
	wal.DB().Close()
	wal.entryCache.Purge()
	//wal.ents = make([]aspirapb.EntryMeta, 1, 2000)
	wal.snapshot = raftpb.Snapshot{}
}

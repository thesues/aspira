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
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"
	"github.com/thesues/aspira/protos/aspirapb"
	"github.com/thesues/aspira/utils"
	"github.com/thesues/aspira/xlog"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/lump"
	cannyls "github.com/thesues/cannyls-go/storage"
)

type WAL struct {
	p   unsafe.Pointer //the type is *cannyls.Storage
	ab  *block.AlignedBytes
	sab *block.AlignedBytes

	//sab        *block.AlignedBytes
	dbLock     *sync.Mutex //protect readCounts
	readCounts int
	// Protects access to all fields. Most methods of MemoryStorage are
	sync.Mutex
	snapshot raftpb.Snapshot
	// ents[i] has raft log position i+snapshot.Metadata.Index
	firstIndex uint64
	lastIndex  uint64
	cache      *FifoCache
	lastWrite  int64 //seconds
}

var (
	keyMask                = (^uint64(0) >> 2) //0x3FFFFFFFFFFFFFFF, the first two bits are zero
	minimalKey             = ^keyMask
	maxKey                 = keyMask
	endOfList              = errors.Errorf("end of list of keys")
	errNotFound            = errors.New("Unable to find raft entry")
	throttle               = (40 << 10)
	snapshotCatchUpEntries = uint64(10000)
)

func Init(db *cannyls.Storage) *WAL {

	wal := &WAL{
		ab:         block.NewAlignedBytes(512, block.Min()),
		sab:        block.NewAlignedBytes(512, block.Min()),
		readCounts: 0,
		dbLock:     new(sync.Mutex),
		snapshot:   raftpb.Snapshot{},
		lastWrite:  time.Now().Unix(),
		cache:      NewFifoCache(16),
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

	if err = wal.addEntries(entries); err != nil {
	}
	if err = wal.setHardState(hd); err != nil {
		return err
	}
	return nil
}

//reset for test only
func (wal *WAL) reset(entries []raftpb.Entry) {
	wal.deleteFrom(0)
	for _, e := range entries {
		var m aspirapb.EntryMeta
		switch e.Type {
		case raftpb.EntryConfChange:
			m = aspirapb.EntryMeta{Index: e.Index, Term: e.Term, EntryType: aspirapb.EntryMeta_ConfChange, Data: e.Data}
		case raftpb.EntryNormal:
			m = aspirapb.EntryMeta{Index: e.Index, Term: e.Term, EntryType: aspirapb.EntryMeta_NormalOther, Data: e.Data}
		}
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
	return wal.lastIndex, nil
}

// FirstIndex implements the Storage interface.
func (wal *WAL) FirstIndex() (uint64, error) {
	wal.Lock()
	defer wal.Unlock()
	return wal.firstIndex, nil
}

func (wal *WAL) addEntries(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	wal.Lock()
	defer wal.Unlock()

	xlog.Logger.Infof("save entries %d => %d", entries[0].Index, entries[len(entries)-1].Index)

	first := wal.firstIndex
	last := entries[0].Index + uint64(len(entries)) - 1
	// shortcut if there is no new entry.
	if last < first {
		return nil
	}

	// truncate compacted entries
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}

	//offset := entries[0].Index - wal.ents[0].Index

	if wal.lastIndex <= entries[0].Index {
		//have overlap with current log, truncate current log
		//wal.ents = append([]aspirapb.EntryMeta{}, wal.ents[:offset]...)
		//remove cache
		wal.cache.PurgeFrom(entries[0].Index)
		wal.deleteFrom(entries[0].Index)
	} else if wal.lastIndex+1 == entries[0].Index {
		//normal append
	} else {
		xlog.Logger.Panicf("missing log entry [last: %d, append at: %d]",
			wal.lastIndex, entries[0].Index)
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
		wal.lastIndex = entryMeta.Index
		wal.cache.Add(e)
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

func (wal *WAL) readFirstIndex() (uint64, bool) {
	idx, err := wal.DB().First(wal.EntryKey(0))
	if err != nil {
		return 0, false
	}
	return (idx.U64() & keyMask) + 1, true
}

func (wal *WAL) readLastIndex() (uint64, bool) {
	id, ok := wal.DB().MaxId()
	if !ok || id.U64() < minimalKey {
		return 0, false
	}
	return id.U64() & keyMask, true

}

//PastLife load entries into memory and return if it is a RESTART
func (wal *WAL) PastLife() (bool, error) {
	xlog.Logger.Info("replaying WAL")

	//xlog.Logger.Info("run JournalGC")
	//wal.DB().JournalGC()
	//xlog.Logger.Info("end run JournalGC")

	data, err := wal.DB().Get(wal.snapshotKey())
	if err != nil {
		idx, ok := wal.readFirstIndex()
		if !ok {
			//if wal is emtpy, initialize db, and set
			wal.firstIndex = 1
			wal.lastIndex = 0
			//write empty Entry to keep the invarant rule
			e := raftpb.Entry{}
			data, err := e.Marshal()
			if _, err = wal.DB().PutEmbed(wal.EntryKey(0), data); err != nil {
				return false, err
			}
			return false, nil
		}

		wal.firstIndex = idx
		idx, ok = wal.readLastIndex()
		if !ok {
			return false, errors.Errorf("if wal has a snapshot, it must have a fake snapshot entry")
		}
		wal.lastIndex = idx
		return true, nil
	}

	//has snapshot
	var snap raftpb.Snapshot
	err = snap.Unmarshal(data)
	if err != nil {
		return false, err
	}
	wal.snapshot = snap
	wal.firstIndex = snap.Metadata.Index + 1

	idx, ok := wal.readLastIndex()
	if !ok {
		return false, errors.Errorf("if wal has a snapshot, it must have a fake snapshot entry")
		//wal.lastIndex = snap.Metadata.Index
	}
	wal.lastIndex = idx
	//read lastIndex from DB

	return true, nil
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

	xlog.Logger.Infof("all entries %d => %d", lo, hi)
	wal.Lock()
	defer wal.Unlock()

	if lo < wal.firstIndex {
		return nil, raft.ErrCompacted
	}
	if hi > wal.lastIndex+1 {
		xlog.Logger.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, wal.lastIndex)
	}
	// only contains dummy entries.
	if wal.lastIndex == 0 {
		return nil, raft.ErrUnavailable
	}
	size := 0
	for _, id := range wal.DB().ListRange(wal.EntryKey(lo), wal.EntryKey(hi), 100) {
		if v, ok := wal.cache.Get(id.U64() & keyMask); ok {
			es = append(es, v)
			size += v.Size()
			xlog.Logger.Debugf("cache hit for %d", id.U64()&keyMask)
		} else {
			xlog.Logger.Debugf("cache missing for %d", id.U64()&keyMask)
			data, err := wal.DB().Get(id)
			if err != nil {
				xlog.Logger.Fatalf("failed to get id %+v, err is %+v", id, err)
			}
			var meta aspirapb.EntryMeta
			if err = meta.Unmarshal(data); err != nil {
				xlog.Logger.Fatalf("Unmarshal data failed %+v", err)
			}
			e := wal.fromMetaToRaftEntry(meta)
			size += e.Size()
			//wal.entryCache.Add(e.Index, e)
			es = append(es, e)
		}
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

func (wal *WAL) Term(i uint64) (uint64, error) {
	wal.Lock()
	defer wal.Unlock()
	return wal.term(i)
}

// Term implements the Storage interface.
func (wal *WAL) term(i uint64) (uint64, error) {
	if i < wal.firstIndex {
		return 0, raft.ErrCompacted
	}

	if i > wal.lastIndex {
		return 0, raft.ErrUnavailable
	}

	e, ok := wal.cache.Get(i)
	if ok {
		return e.Term, nil
	}
	xlog.Logger.Infof("log missing %d", i)
	var meta aspirapb.EntryMeta
	data, err := wal.DB().Get(wal.EntryKey(i))
	if err != nil {
		return 0, raft.ErrUnavailable
	}
	if err = meta.Unmarshal(data); err != nil {
		return 0, err
	}
	return meta.Term, nil
}

func (wal *WAL) ApplySnapshot(snap raftpb.Snapshot) {
	wal.Lock()
	defer wal.Unlock()
	if raft.IsEmptySnap(snap) {
		return
	}
	wal.snapshot = snap
	wal.firstIndex = snap.Metadata.Index + 1
	wal.lastIndex = snap.Metadata.Index
	wal.cache.PurgeFrom(0)

	//wal.ents = []aspirapb.EntryMeta{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}	//save to disk.

	//wal.DB().JournalGC()
	wal.deleteFrom(snap.Metadata.Index)
	//set log value which represent snapshot.Term, snapshot.Index
	e := raftpb.Entry{Term: snap.Metadata.Term, Index: snap.Metadata.Index}
	data, err := e.Marshal()
	if _, err = wal.DB().PutEmbed(wal.EntryKey(e.Index), data); err != nil {
		return
	}
	//set snapshot key
	data, _ = wal.snapshot.Marshal()
	wal.DB().PutEmbed(wal.snapshotKey(), data)
}

func (wal *WAL) compact(snapi uint64) {
	if wal.InflightSnapshot() {
		return
	}

	compactIndex := uint64(1)
	if snapi > snapshotCatchUpEntries {
		compactIndex = snapi - snapshotCatchUpEntries
	}

	if compactIndex < wal.firstIndex {
		return
	}

	xlog.Logger.Infof("snapshot compact log at [%d]", compactIndex)
	//compact cannyls

	//disk
	if err := wal.deleteUntil(compactIndex); err != nil {
		xlog.Logger.Error(err.Error())
	}
	wal.firstIndex = compactIndex + 1
}

//CreateSnapshot, if createSnapshot
// is done, it means we have synced the database, so the raft worker do not have to sync again
func (wal *WAL) CreateSnapshot(snapi uint64, cs *raftpb.ConfState, udata []byte) (created bool, err error) {

	wal.Lock()
	defer wal.Unlock()
	if snapi <= wal.snapshot.Metadata.Index {
		return false, raft.ErrSnapOutOfDate
	}

	if snapi > wal.lastIndex {
		return false, errors.Errorf("snapshot %d is out of bound lastindex(%d)", snapi, wal.lastIndex)
	}

	t, err := wal.term(snapi)
	if err != nil {
		return false, err
	}

	wal.snapshot.Metadata.Index = snapi
	wal.snapshot.Metadata.Term = t
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

	wal.compact(snapi)
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
	wal.cache.PurgeFrom(0)
	wal.snapshot = raftpb.Snapshot{}
}

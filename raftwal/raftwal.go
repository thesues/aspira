/*
 * Copyright 2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package raftwal

import (
	"io"
	"sync"
	"sync/atomic"
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
	p          unsafe.Pointer //the type is *cannyls.Storage
	ab         *block.AlignedBytes
	cache      *sync.Map
	dbLock     *sync.Mutex //protect readCounts
	readCounts int
	entryCache *FifoCache
}

var (
	_firstKey              = "firstKey"
	_lastKey               = "lastKey"
	_snapshotKey           = "snapshotKey"
	keyMask                = (^uint64(0) >> 2) //0x3FFFFFFFFFFFFFFF, the first two bits are zero
	minimalKey             = ^keyMask
	maxKey                 = keyMask
	endOfList              = errors.Errorf("end of list of keys")
	errNotFound            = errors.New("Unable to find raft entry")
	snapshotCatchUpEntries = uint64(10000)
)

func Init(db *cannyls.Storage) *WAL {

	wal := &WAL{
		ab:         block.NewAlignedBytes(512, block.Min()),
		readCounts: 0,
		dbLock:     new(sync.Mutex),
		cache:      new(sync.Map),
		entryCache: NewFifoCache(128),
	}
	atomic.StorePointer(&wal.p, unsafe.Pointer(db))

	snap, err := wal.Snapshot()
	if err != nil {
		panic("failed to read snapshot")
	}

	if !raft.IsEmptySnap(snap) { //if have snapshot
		return wal
	}

	_, err = wal.FirstIndex()
	if err == errNotFound {
		ents := make([]raftpb.Entry, 1)
		ents[0].Type = raftpb.EntryNormal
		wal.reset(ents)
	}

	//if has snapshot, run DeleteUntil()
	//optional
	if err = wal.deleteUntil(snap.Metadata.Index); err != nil {
		panic("raftwal: Init failed")
	}
	wal.cache.Store(_firstKey, snap.Metadata.Index+1)

	return wal
}

/*
func (wal *WAL) memberShipKey() (ret lump.LumpId) {
	var buf [8]byte
	buf[0] = 0x80
	copy(buf[1:], []byte("confKey"))
	ret, err := lump.FromBytes(buf[:])
	if err != nil {
		panic("memberShipKey failed")
	}
	return
}
*/

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
		return
	}
	if err = wal.addEntries(entries, true); err != nil {
		return
	}
	return
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
	return
}

func (wal *WAL) Snapshot() (snap raftpb.Snapshot, err error) {
	//cache
	if val, ok := wal.cache.Load(_snapshotKey); ok {
		snap, ok := val.(raftpb.Snapshot)
		if ok && !raft.IsEmptySnap(snap) {
			return snap, nil
		}
	}

	data, err := wal.DB().Get(wal.snapshotKey())
	if err != nil {
		return snap, nil //empty snapshot
	}
	err = snap.Unmarshal(data)
	return snap, nil
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

func (wal *WAL) FirstIndex() (uint64, error) {
	if val, ok := wal.cache.Load(_firstKey); ok {
		if first, ok := val.(uint64); ok {
			return first, nil
		}
	}
	firstIdx, err := wal.DB().First(wal.EntryKey(0))
	if err != nil {
		return 0, errNotFound
	}

	//mask
	i := firstIdx.U64() & keyMask
	wal.cache.Store(_firstKey, i+1)
	return i + 1, nil
}

func (wal *WAL) LastIndex() (uint64, error) {
	id, ok := wal.DB().MaxId()
	if !ok || id.U64() < minimalKey {
		return 0, errNotFound
	}
	ret := id.U64() & keyMask
	return ret, nil
}

func (wal *WAL) addEntries(entries []raftpb.Entry, check bool) error {
	if len(entries) == 0 {
		return nil
	}
	var last uint64 = 0
	if check {
		firstIndex, err := wal.FirstIndex() //atomic get
		if err != nil {
			return err
		}

		if entries[len(entries)-1].Index < firstIndex {
			//warning
			return nil
		}

		if firstIndex > entries[0].Index {
			entries = entries[firstIndex-entries[0].Index:]
		}

		last, err = wal.LastIndex() //atomic get
		if err != nil {
			return err
		}
	}

	for _, e := range entries {
		var entryMeta aspirapb.EntryMeta
		entryMeta.Term = e.Term
		entryMeta.Index = e.Index
		switch e.Type {
		case raftpb.EntryNormal:
			if len(e.Data) != 0 {
				var proposal aspirapb.AspiraProposal
				proposal.Unmarshal(e.Data)
				if proposal.ProposalType == aspirapb.AspiraProposal_Put {
					entryMeta.EntryType = aspirapb.EntryMeta_Put
					entryMeta.AssociateKey = proposal.AssociateKey
					wal.ab.Resize(uint32(len(proposal.Data)))
					copy(wal.ab.AsBytes(), proposal.Data)
					xlog.Logger.Debugf("Wrote Ext data %x\n\n", wal.ExtKey(entryMeta.Index).U64())
					_, err := wal.DB().Put(wal.ExtKey(entryMeta.Index), lump.NewLumpDataWithAb(wal.ab))
					if err != nil {
						return err
					}
				} else {
					entryMeta.EntryType = aspirapb.EntryMeta_PutWithOffset
					entryMeta.Data = e.Data
				}
			} else {
				entryMeta.EntryType = aspirapb.EntryMeta_LeaderCommit
				entryMeta.Data = e.Data
			}
		case raftpb.EntryConfChange:
			entryMeta.EntryType = aspirapb.EntryMeta_ConfChange
			entryMeta.Data = e.Data
		default:
			panic("meet new type")
		}

		data, err := entryMeta.Marshal()
		if err != nil {
			return err
		}
		if _, err = wal.DB().PutEmbed(wal.EntryKey(entryMeta.Index), data); err != nil {
			return err
		}
		xlog.Logger.Infof("cached %d", e.Index)
		wal.entryCache.Add(e)
	}

	laste := entries[len(entries)-1].Index
	wal.cache.Store(_lastKey, laste)
	if laste < last {
		wal.entryCache.PurgeFrom(laste + 1)
		return wal.deleteFrom(laste + 1)
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

func (wal *WAL) PastLife() bool {

	snap, _ := wal.Snapshot()
	if !raft.IsEmptySnap(snap) {
		return true
	}
	_, err := wal.hardState()
	if err != nil {
		return false
	}
	first, _ := wal.FirstIndex()
	last, _ := wal.LastIndex()

	return last >= first

}

func (wal *WAL) AllEntries(lo, hi, maxSize uint64) (es []raftpb.Entry, err error) {

	xlog.Logger.Debugf("AllEntries from %d => %d, maxSize is %d", lo, hi, maxSize)
	size := 0
	for _, id := range wal.DB().ListRange(wal.EntryKey(lo), wal.EntryKey(hi), 100) {
		if v, ok := wal.entryCache.Get(id.U64() & keyMask); ok {
			es = append(es, v)
			size += v.Size()

		} else {
			data, err := wal.DB().Get(id)
			if err != nil {
				xlog.Logger.Fatalf("failed to get id %+v, err is %+v", id, err)
			}
			var meta aspirapb.EntryMeta
			var e raftpb.Entry
			if err = meta.Unmarshal(data); err != nil {
				xlog.Logger.Fatalf("Unmarshal data failed %+v", err)
			}
			switch meta.EntryType {
			case aspirapb.EntryMeta_PutWithOffset:
				e.Type = raftpb.EntryNormal
				e.Data = meta.Data
			case aspirapb.EntryMeta_Put:
				//build data
				e.Type = raftpb.EntryNormal
				extData, err := wal.DB().Get(wal.ExtKey(id.U64() & keyMask))
				if err != nil {
					xlog.Logger.Fatalf("Get data failed %+v", err)

				}
				//restore the proposal data from EntryMeta
				var proposal aspirapb.AspiraProposal
				proposal.AssociateKey = meta.AssociateKey
				proposal.Data = extData
				data, err := proposal.Marshal()
				utils.Check(err)
				//if len(extData) > 0 {
				e.Data = data
				//}
			case aspirapb.EntryMeta_LeaderCommit:
				e.Type = raftpb.EntryNormal
				e.Data = nil
			case aspirapb.EntryMeta_ConfChange:
				e.Type = raftpb.EntryConfChange
				//if len(meta.Data) > 0 {
				e.Data = meta.Data
				//}
			default:
				xlog.Logger.Fatalf("unknow type read from %+v, meta : %+v", id, meta)
			}
			e.Term = meta.Term
			e.Index = meta.Index

			size += e.Size()
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

func (wal *WAL) reset(es []raftpb.Entry) error {
	wal.deleteFrom(0)
	wal.addEntries(es, false)
	return nil
}

func (wal *WAL) deleteUntil(until uint64) error {
	if err := wal.DB().DeleteRange(wal.EntryKey(0), wal.EntryKey(until), true); err != nil {
		return err
	}
	/*
		if err := wal.DB().DeleteRange(wal.ExtKey(0), wal.ExtKey(until), true); err != nil {
			return err
		}
	*/
	return nil

}

/*
	Term returns the term of entry i, which must be in the range
	// [FirstIndex()-1, LastIndex()]
*/
func (wal *WAL) Term(idx uint64) (uint64, error) {

	first, err := wal.FirstIndex()
	if err != nil {
		return 0, err
	}
	if idx < first-1 {
		return 0, raft.ErrCompacted
	}

	e, ok := wal.entryCache.Get(idx)
	if ok {
		return e.Term, nil
	}

	xlog.Logger.Infof("TERM:cache missing %d", idx)
	var meta aspirapb.EntryMeta
	data, err := wal.DB().Get(wal.EntryKey(idx))
	if err != nil {
		return 0, raft.ErrUnavailable
	}
	if err = meta.Unmarshal(data); err != nil {
		return 0, err
	}

	if idx < meta.Index {
		return 0, raft.ErrCompacted
	}
	return meta.Term, nil

}

//if CreateSnapshot is done, it means we have synced the database, so the raft worker do not have to sync again
func (wal *WAL) CreateSnapshot(i uint64, cs *raftpb.ConfState, udata []byte) (created bool, err error) {

	var snap raftpb.Snapshot
	first, err := wal.FirstIndex()
	if err != nil {
		return
	}
	if i < first {
		err = raft.ErrSnapOutOfDate
		return
	}

	var em aspirapb.EntryMeta
	data, err := wal.DB().Get(wal.EntryKey(i))
	if err != nil {
		return
	}
	if err = em.Unmarshal(data); err != nil {
		return
	}

	snap.Metadata.Index = i
	snap.Metadata.Term = em.Term
	snap.Metadata.ConfState = *cs
	snap.Data = udata

	//set snapshot key
	data, err = snap.Marshal()
	if err != nil {
		return
	}

	//TODO: if wal.DB() had a snapshot, delete it, FIXME
	wal.DB().Sync()
	//wal.entryCache.PurgeFrom(0)
	if _, err = wal.DB().PutEmbed(wal.snapshotKey(), data); err != nil {
		return
	}
	//keep the origin data
	//set log value which represent snapshot.Term, snapshot.Index
	/*
		e := raftpb.Entry{Term: snap.Metadata.Term, Index: snap.Metadata.Index}
		data, err = e.Marshal()
		if _, err = wal.DB().PutEmbed(wal.EntryKey(e.Index), data); err != nil {
			return
		}
	*/

	wal.cache.Store(_snapshotKey, snap)
	//compact parts
	wal.compact(snap.Metadata.Index)

	return true, nil
}

func (wal *WAL) compact(snapi uint64) {
	if wal.InflightSnapshot() {
		return
	}

	compactIndex := uint64(1)
	if snapi > snapshotCatchUpEntries {
		compactIndex = snapi - snapshotCatchUpEntries
	}

	first, _ := wal.FirstIndex()
	if compactIndex < first {
		return
	}

	xlog.Logger.Infof("snapshot compact log at [%d]", compactIndex)
	//compact cannyls

	//disk, leave compactIndex
	if err := wal.deleteUntil(compactIndex); err != nil {
		xlog.Logger.Error(err.Error())
	}
	//new first is
	wal.cache.Store(_firstKey, compactIndex+1)
}

func (wal *WAL) ApplySnapshot(snap raftpb.Snapshot) {
	if raft.IsEmptySnap(snap) {
		return
	}
	//wal.ents = []aspirapb.EntryMeta{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}	//save to disk.

	//wal.DB().JournalGC()

	wal.deleteFrom(snap.Metadata.Index)

	//set log value which represent snapshot.Term, snapshot.Index
	/*
		e := raftpb.Entry{Term: snap.Metadata.Term, Index: snap.Metadata.Index}
		data, err := e.Marshal()
		if _, err = wal.DB().PutEmbed(wal.EntryKey(e.Index), data); err != nil {
			return
		}*/

	//set snapshot key
	data, err := snap.Marshal()
	utils.Check(err)
	wal.DB().PutEmbed(wal.snapshotKey(), data)

	meta := aspirapb.EntryMeta{Term: snap.Metadata.Term, Index: snap.Metadata.Index, EntryType: aspirapb.EntryMeta_LeaderCommit}
	data, err = meta.Marshal()
	utils.Check(err)
	if _, err = wal.DB().PutEmbed(wal.EntryKey(meta.Index), data); err != nil {
		return
	}

	//set lastkey, firstkey, snapshot cache
	wal.cache.Store(_snapshotKey, snap)
	wal.cache.Store(_firstKey, snap.Metadata.Index+1)
	wal.cache.Store(_lastKey, snap.Metadata.Index)
}

func (wal *WAL) InflightSnapshot() bool {
	wal.dbLock.Lock()
	wal.dbLock.Unlock()
	return wal.readCounts > 0
}

/*
ApplyPut and ApplyPutWithOffset do not need a DB lock, because before receiving the snapshot,
worker will drain the applyMessage channel
*/
func (wal *WAL) ApplyPut(index uint64) error {

	dataPortion, err := wal.DB().GetRecord(wal.ExtKey(index))
	if err != nil {
		return err
	}
	return wal.DB().WriteRecord(lump.FromU64(0, index), *dataPortion)
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
	wal.entryCache.PurgeFrom(0)
	wal.cache = new(sync.Map)
}

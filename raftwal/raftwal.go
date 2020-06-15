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
	"sync"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"
	"github.com/thesues/aspira/protos/aspirapb"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/lump"
	cannyls "github.com/thesues/cannyls-go/storage"
)

type WAL struct {
	db    *cannyls.Storage
	cache *sync.Map
}

var (
	snapshotKey = "snapshot"
	firstKey    = "first"
	lastKey     = "last"
	keyMask     = (^uint64(0) >> 2) //0x3FFFFFFFFFFFFFFF
	MinimalKey  = ^keyMask
	MaxKey      = keyMask
	endOfList   = errors.Errorf("end of list of keys")
	errNotFound = errors.New("Unable to find raft entry")
)

func Init(db *cannyls.Storage) *WAL {
	wal := &WAL{
		db:    db,
		cache: new(sync.Map),
	}

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
		ents[0].Type = raftpb.EntryConfChange
		wal.reset(ents)
	}

	//if has snapshot, run DeleteUntil()
	//optional
	if err = wal.deleteUntil(snap.Metadata.Index); err != nil {
		panic("raftwal: Init failed")
	}
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
		return
	}
	if err = wal.addEntries(entries); err != nil {
		return
	}
	return
}

func (wal *WAL) Sync() {
	wal.db.JournalSync()
}

func (wal *WAL) HardState() (hd raftpb.HardState, err error) {
	data, err := wal.db.Get(wal.hardStateKey())
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
	_, err = wal.db.PutEmbed(wal.hardStateKey(), data)
	return
}

func (wal *WAL) Snapshot() (snap raftpb.Snapshot, err error) {

	data, err := wal.db.Get(wal.snapshotKey())
	if err != nil {
		return snap, nil //empty snapshot
	}
	err = snap.Unmarshal(data)
	return
}

/*
func (wal *WAL) setSnapshot(snap raftpb.Snapshot) error {
	if raft.IsEmptySnap(snap) {
		return nil
	}

	data, err := snap.Marshal()
	if err != nil {
		return errors.Wrapf(err, "wal.Store: While marshal snapshot")
	}
	if _, err = wal.db.PutEmbed(wal.snapshotKey(), data); err != nil {
		return errors.Wrapf(err, "wal.Store: failed to write data")
	}

	e := pb.EntryMeta{Term: snap.Metadata.Term, Index: snap.Metadata.Index}
	data, err = e.Marshal()
	if err != nil {
		return err
	}
	wal.db.PutEmbed(wal.EntryKey(e.Index), data)
	// Update the last index cache here. This is useful so when a follower gets a jump due to
	// receiving a snapshot and Save is called, addEntries wouldn't have much. So, the last index
	// cache would need to rely upon this update here.
	if val, ok := wal.cache.Load(lastKey); ok {
		le := val.(uint64)
		if le < snap.Metadata.Index {
			wal.cache.Store(lastKey, snap.Metadata.Index)
		}
	}
	// Cache snapshot.
	wal.cache.Store(snapshotKey, &snap)
	return nil

}
*/

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
	snap, _ := wal.Snapshot() //will never return error for now
	if !raft.IsEmptySnap(snap) {
		return snap.Metadata.Index + 1, nil
	}

	firstIdx, err := wal.db.First(wal.EntryKey(0))
	if err != nil {
		return 0, errNotFound
	}
	//mask
	firstIndex := firstIdx.U64() & keyMask

	return firstIndex + 1, nil
}

func (wal *WAL) LastIndex() (uint64, error) {
	id, ok := wal.db.MaxId()
	if !ok || id.U64() < MinimalKey {
		return 0, errNotFound
	}
	ret := id.U64() & keyMask
	return ret, nil
}

/*
Term
Index
Type
Data
index => {TERM, INDEX, TYPE, DATA}
if TYPE is NORMAL split two.
else save all
*/
func (wal *WAL) addEntries(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

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

	last, err := wal.LastIndex() //atomic get
	if err != nil {
		return err
	}

	var entryMeta aspirapb.EntryMeta
	ab := block.NewAlignedBytes(512, block.Min())
	for _, e := range entries {
		entryMeta.Term = e.Term
		entryMeta.Index = e.Index

		switch e.Type {
		case raftpb.EntryNormal:
			if len(e.Data) != 0 {
				entryMeta.EntryType = aspirapb.EntryMeta_Put
				ab.Resize(uint32(len(e.Data)))
				copy(ab.AsBytes(), e.Data)
				wal.db.Put(wal.ExtKey(entryMeta.Index), lump.NewLumpDataWithAb(ab))
			} else {
				entryMeta.EntryType = aspirapb.EntryMeta_LeaderCommit
			}
		case raftpb.EntryConfChange:
			entryMeta.EntryType = aspirapb.EntryMeta_ConfChange
		default:
			panic("meet new type")
		}

		data, err := entryMeta.Marshal()
		if err != nil {
			return err
		}
		if _, err = wal.db.PutEmbed(wal.EntryKey(entryMeta.Index), data); err != nil {
			return err
		}
	}

	laste := entries[len(entries)-1].Index
	wal.cache.Store(lastKey, laste)

	if laste < last {
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
	if err := wal.db.DeleteRange(logStart, logEnd, false); err != nil {
		return err
	}

	if err := wal.db.DeleteRange(exLogStart, exLogEnd, true); err != nil {
		return err
	}
	return nil
}

func (wal *WAL) InitialState() (hs raftpb.HardState, cs raftpb.ConfState, err error) {
	hs, err = wal.HardState()
	if err != nil {
		return
	}
	var snap raftpb.Snapshot
	snap, err = wal.Snapshot()
	if err != nil {
		return
	}
	return hs, snap.Metadata.ConfState, nil
}

//max range of [lo, hi) is [0, keyMask)
func (wal *WAL) allEntries(lo, hi, maxSize uint64) (es []raftpb.Entry, err error) {
	var meta aspirapb.EntryMeta
	var e raftpb.Entry
	size := 0
	err = wal.db.RangeIter(wal.EntryKey(lo), wal.EntryKey(hi), func(id lump.LumpId, data []byte) error {
		if err = meta.Unmarshal(data); err != nil {
			return err
		}
		switch meta.EntryType {

		case aspirapb.EntryMeta_Put:
			//build data
			e.Type = raftpb.EntryNormal
			extData, err := wal.db.Get(wal.ExtKey(id.U64() & keyMask))
			if err != nil {
				return err
			}
			e.Data = extData
		case aspirapb.EntryMeta_LeaderCommit:
			e.Type = raftpb.EntryNormal
			e.Data = nil
		default:
			e.Type = raftpb.EntryConfChange
			if len(meta.Data) > 0 {
				e.Data = meta.Data
			}
		}
		e.Term = meta.Term
		e.Index = meta.Index

		size += e.Size()
		es = append(es, e)
		//if maxSize is 0, we still want to return at lease one entry
		if uint64(size) > maxSize {
			n := len(es)
			if n > 1 {
				es = es[:n-1]
			}
			return endOfList
		}
		return nil
	}, true)

	//if we met unexpected error
	if err != nil && err != endOfList {
		return nil, err
	}
	return es, nil
}

func (wal *WAL) Entries(lo, hi, maxSize uint64) (es []raftpb.Entry, err error) {
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
	return wal.allEntries(lo, hi, maxSize)
}

func (wal *WAL) reset(es []raftpb.Entry) error {
	wal.deleteFrom(0)
	var entryMeta aspirapb.EntryMeta
	ab := block.NewAlignedBytes(512, block.Min())
	for _, e := range es {
		entryMeta.Term = e.Term
		entryMeta.Index = e.Index
		if e.Type == raftpb.EntryNormal {
			//parse data, get
			entryMeta.EntryType = aspirapb.EntryMeta_Put
			ab.Resize(uint32(len(e.Data)))
			copy(ab.AsBytes(), e.Data)
			wal.db.Put(wal.ExtKey(entryMeta.Index), lump.NewLumpDataWithAb(ab))
		} else {
			entryMeta.EntryType = aspirapb.EntryMeta_ConfChange
		}

		data, err := entryMeta.Marshal()
		if err != nil {
			return err
		}
		if _, err = wal.db.PutEmbed(wal.EntryKey(entryMeta.Index), data); err != nil {
			return err
		}
	}
	return nil
}

func (wal *WAL) deleteUntil(until uint64) error {
	return wal.db.RangeIter(wal.EntryKey(0), wal.EntryKey(until), func(id lump.LumpId, data []byte) error {
		if _, _, err := wal.db.Delete(id); err != nil {
			return err
		}
		/*
			if isDeleteExtLog {
				wal.db.Delete(wal.ExtKey(id.U64() & keyMask))
			}
		*/
		return nil
	}, false)
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

	var e aspirapb.EntryMeta
	data, err := wal.db.Get(wal.EntryKey(idx))
	if err != nil {
		return 0, raft.ErrUnavailable
	}
	if err = e.Unmarshal(data); err != nil {
		return 0, err
	}

	if idx < e.Index {
		return 0, raft.ErrCompacted
	}
	return e.Term, nil
}

func (wal *WAL) CreateSnapshot(i uint64, cs *raftpb.ConfState, udata []byte) (snap raftpb.Snapshot, err error) {
	first, err := wal.FirstIndex()
	if err != nil {
		return
	}
	if i < first {
		err = raft.ErrSnapOutOfDate
		return
	}

	var em aspirapb.EntryMeta
	data, err := wal.db.Get(wal.EntryKey(i))
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
	wal.db.PutEmbed(wal.snapshotKey(), data)

	//set log value which represent snapshot.Term, snapshot.Index
	e := raftpb.Entry{Term: snap.Metadata.Term, Index: snap.Metadata.Index}
	data, err = e.Marshal()
	if _, err = wal.db.PutEmbed(wal.EntryKey(e.Index), data); err != nil {
		return
	}
	if err = wal.deleteUntil(snap.Metadata.Index); err != nil {
		return
	}
	return
}

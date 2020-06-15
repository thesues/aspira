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
	"math"
	"os"
	"reflect"
	"testing"

	raft "github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/stretchr/testify/assert"
	cannyls "github.com/thesues/cannyls-go/storage"
)

func TestStorageTerm(t *testing.T) {
	ents := []raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	tests := []struct {
		i uint64

		werr   error
		wterm  uint64
		wpanic bool
	}{
		{2, raft.ErrCompacted, 0, false},
		{3, nil, 3, false},
		{4, nil, 4, false},
		{5, nil, 5, false},
		{6, raft.ErrUnavailable, 0, false},
	}

	db, err := cannyls.CreateCannylsStorage("wal.lusf", 10<<20, 0.1)
	defer os.Remove("wal.lusf")
	assert.Nil(t, err)
	wal := Init(db)

	for i, tt := range tests {
		wal.reset(ents)

		func() {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wpanic {
						t.Errorf("%d: panic = %v, want %v", i, true, tt.wpanic)
					}
				}
			}()

			term, err := wal.Term(tt.i)
			if err != tt.werr {
				t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
			}
			if term != tt.wterm {
				t.Errorf("#%d: term = %d, want %d", i, term, tt.wterm)
			}
		}()
	}
}

func TestStorageEntries(t *testing.T) {
	ents := []raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}
	tests := []struct {
		lo, hi, maxsize uint64

		werr     error
		wentries []raftpb.Entry
	}{
		{2, 6, math.MaxUint64, raft.ErrCompacted, nil},
		{3, 4, math.MaxUint64, raft.ErrCompacted, nil},
		{4, 5, math.MaxUint64, nil, []raftpb.Entry{{Index: 4, Term: 4}}},
		{4, 6, math.MaxUint64, nil, []raftpb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		{4, 7, math.MaxUint64, nil, []raftpb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}},
		// even if maxsize is zero, the first entry should be returned
		{4, 7, 0, nil, []raftpb.Entry{{Index: 4, Term: 4}}},
		// limit to 2
		{4, 7, uint64(ents[1].Size() + ents[2].Size()), nil, []raftpb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		// limit to 2
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size()/2), nil, []raftpb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size() - 1), nil, []raftpb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}}},
		// all
		{4, 7, uint64(ents[1].Size() + ents[2].Size() + ents[3].Size()), nil, []raftpb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 6}}},
	}
	db, err := cannyls.CreateCannylsStorage("wal.lusf", 10<<20, 0.1)
	defer os.Remove("wal.lusf")
	assert.Nil(t, err)
	wal := Init(db)
	for i, tt := range tests {
		wal.reset(ents)
		entries, _ := wal.Entries(tt.lo, tt.hi, tt.maxsize)
		/*
			if err != tt.werr {
				t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
			}
		*/
		assert.Equal(t, entries, tt.wentries)

		if !reflect.DeepEqual(entries, tt.wentries) {
			t.Errorf("#%d: entries = %v, want %v", i, entries, tt.wentries)
		}
	}
}

func TestStorageLastIndex(t *testing.T) {
	ents := []raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	db, err := cannyls.CreateCannylsStorage("wal.lusf", 10<<20, 0.1)
	defer os.Remove("wal.lusf")
	assert.Nil(t, err)
	wal := Init(db)
	wal.reset(ents)
	last, err := wal.LastIndex()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if last != 5 {
		t.Errorf("last = %d, want %d", last, 5)
	}

	wal.addEntries([]raftpb.Entry{{Index: 6, Term: 5}})
	last, err = wal.LastIndex()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if last != 6 {
		t.Errorf("last = %d, want %d", last, 6)
	}
}

func TestStorageFirstIndex(t *testing.T) {
	ents := []raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	db, err := cannyls.CreateCannylsStorage("wal.lusf", 10<<20, 0.1)
	defer os.Remove("wal.lusf")
	assert.Nil(t, err)
	wal := Init(db)
	wal.reset(ents)

	first, err := wal.FirstIndex()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if first != 4 {
		t.Errorf("first = %d, want %d", first, 4)
	}

	var conf raftpb.ConfState
	wal.CreateSnapshot(4, &conf, nil)
	first, err = wal.FirstIndex()
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if first != 5 {
		t.Errorf("first = %d, want %d", first, 5)
	}
}

func TestStorageCreateSnapshot(t *testing.T) {
	ents := []raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	cs := &raftpb.ConfState{Nodes: []uint64{1, 2, 3}}
	data := []byte("data")

	db, err := cannyls.CreateCannylsStorage("wal.lusf", 10<<20, 0.1)
	defer os.Remove("wal.lusf")
	assert.Nil(t, err)
	wal := Init(db)
	wal.reset(ents)

	tests := []struct {
		i uint64

		werr  error
		wsnap raftpb.Snapshot
	}{
		{4, nil, raftpb.Snapshot{Data: data, Metadata: raftpb.SnapshotMetadata{Index: 4, Term: 4, ConfState: *cs}}},
		{5, nil, raftpb.Snapshot{Data: data, Metadata: raftpb.SnapshotMetadata{Index: 5, Term: 5, ConfState: *cs}}},
	}

	for i, tt := range tests {
		wal.reset(ents)
		snap, err := wal.CreateSnapshot(tt.i, cs, data)
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}
		if !reflect.DeepEqual(snap, tt.wsnap) {
			t.Errorf("#%d: snap = %+v, want %+v", i, snap, tt.wsnap)
		}
	}
}

func TestStorageAppend(t *testing.T) {
	db, err := cannyls.CreateCannylsStorage("wal.lusf", 10<<20, 0.1)
	defer os.Remove("wal.lusf")
	assert.Nil(t, err)
	wal := Init(db)

	ents := []raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}}
	tests := []struct {
		entries []raftpb.Entry

		werr     error
		wentries []raftpb.Entry
	}{
		{
			[]raftpb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}},
			nil,
			[]raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}},
		},
		{
			[]raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}},
			nil,
			[]raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}},
		},
		{
			[]raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 6}, {Index: 5, Term: 6}},
			nil,
			[]raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 6}, {Index: 5, Term: 6}},
		},
		{
			[]raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 5}},
			nil,
			[]raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 5}},
		},
		// truncate incoming entries, truncate the existing entries and append
		{
			[]raftpb.Entry{{Index: 2, Term: 3}, {Index: 3, Term: 3}, {Index: 4, Term: 5}},
			nil,
			[]raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 5}},
		},
		// truncate the existing entries and append
		{
			[]raftpb.Entry{{Index: 4, Term: 5}},
			nil,
			[]raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 5}},
		},
		// direct append
		{
			[]raftpb.Entry{{Index: 6, Term: 5}},
			nil,
			[]raftpb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 5}, {Index: 6, Term: 5}},
		},
	}

	for i, tt := range tests {
		wal.reset(ents)
		err := wal.addEntries(tt.entries)
		if err != tt.werr {
			t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
		}
		entries, err := wal.allEntries(0, MaxKey, math.MaxUint64)
		assert.Nil(t, err)
		if !reflect.DeepEqual(entries, tt.wentries) {
			t.Errorf("#%d: entries = %v, want %v", i, entries, tt.wentries)
		}
	}
}

/*
func TestStorageApplySnapshot(t *testing.T) {
	cs := &pb.ConfState{Voters: []uint64{1, 2, 3}}
	data := []byte("data")

	tests := []pb.Snapshot{{Data: data, Metadata: pb.SnapshotMetadata{Index: 4, Term: 4, ConfState: *cs}},
		{Data: data, Metadata: pb.SnapshotMetadata{Index: 3, Term: 3, ConfState: *cs}},
	}

	s := NewMemoryStorage()

	//Apply Snapshot successful
	i := 0
	tt := tests[i]
	err := s.ApplySnapshot(tt)
	if err != nil {
		t.Errorf("#%d: err = %v, want %v", i, err, nil)
	}

	//Apply Snapshot fails due to ErrSnapOutOfDate
	i = 1
	tt = tests[i]
	err = s.ApplySnapshot(tt)
	if err != ErrSnapOutOfDate {
		t.Errorf("#%d: err = %v, want %v", i, err, ErrSnapOutOfDate)
	}
}
*/

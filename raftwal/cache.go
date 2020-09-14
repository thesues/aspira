package raftwal

import (
	"container/list"
	"sync"

	"github.com/coreos/etcd/raft/raftpb"
)

type FifoCache struct {
	capacity int
	q        *list.List
	m        map[uint64]*list.Element
	sync.RWMutex
}

func NewFifoCache(n int) *FifoCache {
	return &FifoCache{
		capacity: n,
		q:        list.New(),
		m:        make(map[uint64]*list.Element),
	}
}

func (fifo *FifoCache) Add(entry raftpb.Entry) {
	fifo.Lock()
	defer fifo.Unlock()
	if fifo.q.Len() == fifo.capacity {
		e := fifo.q.Front()
		fifo.q.Remove(e)
		index := e.Value.(raftpb.Entry).Index
		delete(fifo.m, index)
	}
	e := fifo.q.PushBack(entry)
	fifo.m[entry.Index] = e
}

func (fifo *FifoCache) Get(index uint64) (raftpb.Entry, bool) {
	fifo.RLock()
	defer fifo.RUnlock()
	e, ok := fifo.m[index]
	if !ok {
		return raftpb.Entry{}, false
	}
	return e.Value.(raftpb.Entry), true
}

func (fifo *FifoCache) PurgeFrom(index uint64) {
	fifo.Lock()
	defer fifo.Unlock()
	for i, e := range fifo.m {
		if i >= index {
			e, _ = fifo.m[i]
			delete(fifo.m, i)
			fifo.q.Remove(e)
		}
	}
}

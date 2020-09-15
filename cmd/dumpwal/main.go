package main

import (
	"fmt"
	"math"
	"os"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/thesues/aspira/protos/aspirapb"
	"github.com/thesues/aspira/raftwal"
	"github.com/thesues/aspira/utils"
	"github.com/thesues/aspira/xlog"
	"github.com/thesues/cannyls-go/lump"
	"github.com/thesues/cannyls-go/storage"
	"go.uber.org/zap/zapcore"
)

func snapshotKey() (ret lump.LumpId) {
	//startsWith 0b10XXXXX
	//0x80 + 7 (byte)
	var buf [8]byte
	buf[0] = 0x80
	copy(buf[1:], []byte("snapKey"))
	ret, err := lump.FromBytes(buf[:])
	utils.Check(err)
	return
}

func hardStateKey() (ret lump.LumpId) {
	//startsWith 0b10XXXXX
	//0x80 + 7 (byte)
	var buf [8]byte
	buf[0] = 0x80
	copy(buf[1:], []byte("hardKey"))
	ret, err := lump.FromBytes(buf[:])
	utils.Check(err)
	return
}

var keyMask = (^uint64(0) >> 2) //0x3FFFFFFFFFFFFFFF

func EntryKey(idx uint64) lump.LumpId {
	if idx > keyMask {
		panic("idx is too big")
	}
	idx |= 1 << 63 //first two bit "11"
	idx |= 1 << 62
	return lump.FromU64(0, idx)
}

func ExtKey(idx uint64) lump.LumpId {
	if idx > keyMask {
		panic("idx is too big")
	}
	idx |= 1 << 62 //first two bit "01"
	return lump.FromU64(0, idx)

}

func hasExt(store *storage.Storage, index uint64) bool {
	id := ExtKey(index)
	_, err := store.Get(id)
	if err != nil {
		return false
	}
	return true
}

func memberShipKey() (ret lump.LumpId) {
	var buf [8]byte
	buf[0] = 0x80
	copy(buf[1:], []byte("confKey"))
	ret, err := lump.FromBytes(buf[:])
	if err != nil {
		panic("memberShipKey failed")
	}
	return
}

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: dumpwal <example.lusf>")
		return
	}
	store, err := storage.OpenCannylsStorage(os.Args[1])
	utils.Check(err)

	xlog.InitLog(nil, zapcore.ErrorLevel)
	//snapshot key
	fmt.Printf("SNAPSHOT : ")
	data, err := store.Get(snapshotKey())
	if err == nil {
		var snap raftpb.Snapshot
		utils.Check(snap.Unmarshal(data))
		fmt.Printf("%+v\n", snap.Metadata)
		var memberState aspirapb.MembershipState
		memberState.Unmarshal(snap.Data)
		fmt.Printf("memberstate %+v\n", memberState)
	} else {
		fmt.Printf("\n")
	}
	//
	fmt.Printf("HARDSTAT : ")
	data, err = store.Get(hardStateKey())
	if err == nil {
		var hs raftpb.HardState
		utils.Check(hs.Unmarshal(data))
		fmt.Printf("%+v\n", hs)
	} else {
		fmt.Printf("\n")
	}

	//entries
	wal := raftwal.Init(store)
	wal.PastLife()

	fmt.Printf("ENTRIES: \n")
	first, err := wal.FirstIndex()
	fmt.Printf("FirstIndex : %d\n", first)
	last, err := wal.LastIndex()
	fmt.Printf("LastIndex  : %d\n", last)
	for {
		es, err := wal.AllEntries(first, last+1, math.MaxUint64)
		if err != nil {
			panic(err.Error())
		}
		for i := range es {
			switch es[i].Type {
			case raftpb.EntryConfChange:
				var cc raftpb.ConfChange
				cc.Unmarshal(es[i].Data)
				var ctx aspirapb.RaftContext
				ctx.Unmarshal(cc.Context)

				fmt.Printf("CONFCHANGE: index: %d, term: %d , %+v\n", es[i].Index, es[i].Term, ctx)
			default:
				var p aspirapb.AspiraProposal
				p.Unmarshal(es[i].Data)
				fmt.Printf("index: %d, term: %d , [%+v, %v]\n", es[i].Index, es[i].Term, p.ProposalType, hasExt(store, es[i].Index))
			}

		}
		if last == es[len(es)-1].Index {
			break
		}
		first = es[len(es)-1].Index + 1

	}

	//applied Data
}

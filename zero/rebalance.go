package main

import (
	"sort"

	"github.com/thesues/aspira/protos/aspirapb"
)

type RebalancePolicy interface {
	AllocNewRaftGroup(uint64, int, []*aspirapb.ZeroStoreInfo) []*aspirapb.ZeroStoreInfo
}

type RandomReplication struct{}

func (p RandomReplication) AllocNewRaftGroup(gid uint64, replicate int, current []*aspirapb.ZeroStoreInfo) []*aspirapb.ZeroStoreInfo {
	if len(current) < replicate {
		return nil
	}
	sort.Slice(current, func(a, b int) bool {
		return current[a].EmtpySlots > current[b].EmtpySlots
	})
	var ret []*aspirapb.ZeroStoreInfo
	for i := 0; i < len(current) && len(ret) < 3; i++ {
		if current[i].EmtpySlots > 0 {
			ret = append(ret, current[i])
		} else {
			return nil
		}
	}
	return ret
}

type HashRingReplication struct{}

type CopySetReplication struct{}

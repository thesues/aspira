package main

import (
	"context"

	"github.com/pkg/errors"
	"github.com/thesues/aspira/protos/aspirapb"
)

func (as *AspiraStore) AddWorker(ctx context.Context, request *aspirapb.AddWorkerRequest) (*aspirapb.AddWorkerResponse, error) {
	if w := as.GetWorker(request.Gid) ; w != nil {
		return &aspirapb.AddWorkerResponse{}, errors.Errorf("can not add the same gid into one store")
	}
	err := as.startNewWorker(request.Id, request.Gid, as.addr, request.JoinCluster)
	return &aspirapb.AddWorkerResponse{}, err
}

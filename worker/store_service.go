package main

import (
	"bytes"
	"context"
	"io"
	"time"

	"github.com/pkg/errors"
	"github.com/thesues/aspira/protos/aspirapb"
	"github.com/thesues/aspira/utils"
	"github.com/thesues/aspira/xlog"
)

// AddWorker
func (as *AspiraStore) AddWorker(ctx context.Context, request *aspirapb.AddWorkerRequest) (*aspirapb.AddWorkerResponse, error) {

	//check Gid is unique in  AspiraStore
	if w := as.GetWorker(request.Gid); w != nil {
		return &aspirapb.AddWorkerResponse{}, errors.Errorf("can not add the same gid into one store")
	}

	if request.Type == aspirapb.TnxType_commit {
		err := as.startNewWorker(request.Id, request.Gid, as.addr, request.JoinCluster, request.InitialCluster)
		return &aspirapb.AddWorkerResponse{}, err
	} else if request.Type == aspirapb.TnxType_prepare {
		err := as.savePrepare(request)
		return &aspirapb.AddWorkerResponse{}, err
	}

	xlog.Logger.Errorf("AddworkerRequest type is %v", request.Type)
	return nil, errors.Errorf("AddworkerRequest type is %v", request.Type)
}

// Put
func (as *AspiraStore) Put(ctx context.Context, req *aspirapb.PutRequest) (*aspirapb.PutResponse, error) {
	gid := req.GetGid()
	w := as.GetWorker(gid)
	if w == nil {
		return &aspirapb.PutResponse{}, errors.Errorf("do not have such gid %d", gid)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var p aspirapb.AspiraProposal
	p.Data = req.Payload.Data
	p.ProposalType = aspirapb.AspiraProposal_Put

	start := time.Now()
	index, err := w.proposeAndWait(ctx, &p)
	if err != nil {
		return &aspirapb.PutResponse{}, err
	}
	xlog.Logger.Infof("time eslpated %+v\n", time.Since(start))
	res := aspirapb.PutResponse{
		Gid: gid,
		Oid: index,
	}
	return &res, nil

}

// PutStream
func (as *AspiraStore) PutStream(stream aspirapb.Store_PutStreamServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	gid := req.GetGid()
	w := as.GetWorker(req.GetGid())
	if w == nil {
		return errors.Errorf("do not have such gid %d", gid)
	}
	//TODO check size;

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var p aspirapb.AspiraProposal

	buf := new(bytes.Buffer)
	for {
		req, err = stream.Recv()
		if err != nil && err != io.EOF {
			return err
		}
		if err == io.EOF {
			break
		}
		buf.Write(req.GetPayload().Data)
		if buf.Len() > (0xFFFF*(512) - 2) {
			return errors.Errorf("file too big")
		}
	}
	p.Data = buf.Bytes()
	p.ProposalType = aspirapb.AspiraProposal_Put

	start := time.Now()
	index, err := w.proposeAndWait(ctx, &p)
	if err != nil {
		xlog.Logger.Errorf(err.Error())
		return err
	}
	xlog.Logger.Infof("time eslpated %+v\n", time.Since(start))
	res := aspirapb.PutResponse{
		Gid: gid,
		Oid: index,
	}
	stream.SendAndClose(&res)
	return nil
}

//Get
func (as *AspiraStore) Get(req *aspirapb.GetRequest, stream aspirapb.Store_GetServer) error {

	gid := req.GetGid()
	w := as.GetWorker(req.GetGid())
	if w == nil {
		return errors.Errorf("do not have such gid %d", gid)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	data, err := w.getAndWait(ctx, req.Oid)
	if err != nil {
		xlog.Logger.Errorf(err.Error())
		return err
	}

	n := 0
	var payload aspirapb.Payload
	for {
		size := utils.Min(512<<10, len(data)-n)
		if size == 0 {
			break
		}
		payload.Data = data[n : n+size]
		if err := stream.Send(&payload); err != nil {
			return err
		}
		n += size
	}

	return nil
}

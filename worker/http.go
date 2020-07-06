package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang/glog"
	"github.com/thesues/aspira/protos/aspirapb"
)

func (as *AspiraServer) ServeHTTP() {
	r := gin.Default()

	r.POST("/del/:id", func(c *gin.Context) {
		id, err := strconv.ParseUint(c.Param("id"), 10, 64)
		if err != nil {
			c.String(400, err.Error())
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var p aspirapb.AspiraProposal

		p.ProposalType = aspirapb.AspiraProposal_Delete
		p.Key = id

		start := time.Now()
		_, err = as.proposeAndWait(ctx, &p)
		if err != nil {
			glog.Errorf(err.Error())
			c.String(400, err.Error())
			return
		}
		glog.Infof("time eslpated %+v\n", time.Since(start))
		c.String(200, "delete %d", id)
	})

	r.POST("/put/", func(c *gin.Context) {
		readFile, header, err := c.Request.FormFile("file")
		if err != nil {
			c.String(400, err.Error())
			return
		}
		if header.Size > as.store.ObjectMaxSize() {
			c.String(405, "size too big")
			return
		}
		buf := make([]byte, header.Size, header.Size)
		_, err = io.ReadFull(readFile, buf)
		if err != nil {
			c.String(409, "read failed")
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var p aspirapb.AspiraProposal
		p.Data = buf

		p.ProposalType = aspirapb.AspiraProposal_Put

		start := time.Now()
		index, err := as.proposeAndWait(ctx, &p)
		if err != nil {
			glog.Errorf(err.Error())
			c.String(400, err.Error())
			return
		}
		glog.Infof("time eslpated %+v\n", time.Since(start))
		c.String(200, "wrote to %d", index)
	})

	r.GET("/get/:id", func(c *gin.Context) {
		id, err := strconv.ParseUint(c.Param("id"), 10, 64)
		if err != nil {
			c.String(400, err.Error())
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		data, err := as.getAndWait(ctx, id)
		if err != nil {
			glog.Errorf(err.Error())
			c.String(500, err.Error())
			return
		}
		c.Status(200)
		c.Header("content-length", fmt.Sprintf("%d", len(data)))
		c.Stream(func(w io.Writer) bool {
			_, err := w.Write(data)
			if err != nil {
				fmt.Println(err)
				return true
			}
			return false
		})
	})

	stringID := fmt.Sprintf("%d", as.node.Id)
	srv := &http.Server{
		Addr:    ":808" + stringID,
		Handler: r,
	}

	go func() {
		defer func() {
			glog.Infof("HTTP return")
		}()
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			panic("http server crashed")
		}
	}()

	select {
	case <-as.stopper.ShouldStop():
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			panic("Server Shutdown failed")
		}
		return
	}
}

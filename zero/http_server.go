package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/thesues/aspira/protos/aspirapb"
	"github.com/thesues/aspira/xlog"
)

func (z *Zero) ServHTTP() {
	if z.Cfg.HttpUrl == "" {
		return
	}

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	r.GET("/", func(c *gin.Context) {

		if z.amLeader() {
			c.String(200, z.DisplayStore()+"\n\n"+z.DisplayWorker())
		} else {
			//read from etcd, get the current leader, and redirect
			var memberValue aspirapb.ZeroMemberValue
			kvs, err := EtcdRange(z.Client, electionKeyPrefix)
			if err != nil || len(kvs) == 0 || len(kvs) > 1 || len(kvs[0].Value) == 0 {
				c.String(200, "can not find leader...")
				return
			}

			err = memberValue.Unmarshal(kvs[0].Value)
			if err != nil {
				c.String(200, err.Error())
				return
			}
			//redirect
			c.Redirect(http.StatusTemporaryRedirect, "http://"+memberValue.HttpUrl)
		}

	})

	srv := &http.Server{
		Addr:    z.Cfg.HttpUrl,
		Handler: r,
	}

	go func() {
		defer xlog.Logger.Infof("HTTP return")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			panic("http server crashed")
		}
	}()
}

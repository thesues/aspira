package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/thesues/aspira/xlog"
)

func (z *Zero) ServHTTP() {
	if z.Cfg.HttpUrl == "" {
		return
	}

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	r.GET("/", func(c *gin.Context) {
		c.String(200, z.DisplayStore()+"\n\n"+z.DisplayWorker())
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

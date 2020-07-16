package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"github.com/thesues/aspira/protos/aspirapb"
	_ "github.com/thesues/aspira/worker/docs"
	"github.com/thesues/aspira/xlog"
	cannylsMerics "github.com/thesues/cannyls-go/metrics"
	"go.uber.org/zap"
)

// @Summary Delete an object
// @Param id path integer true "Object ID"
// @Success 200 {string} string ""
// @Failure 400 {string} string ""
// @Router /del/{id} [post]
func (as *AspiraServer) del(c *gin.Context) {
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
		xlog.Logger.Errorf(err.Error())
		c.String(400, err.Error())
		return
	}
	xlog.Logger.Infof("time eslpated %+v\n", time.Since(start))
	c.String(200, "delete %d", id)
}

/*
func (as *AspiraServer) putWithOffset(c *gin.Context) {

}
*/

// @Summary Put an object
// @Accept  multipart/form-data
// @Param   file formData file true  "this is a test file"
// @Success 200 {string} string ""
// @Failure 400 {string} string ""
// @Router /put/ [post]
func (as *AspiraServer) put(c *gin.Context) {
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
		xlog.Logger.Errorf(err.Error())
		c.String(400, err.Error())
		return
	}
	xlog.Logger.Infof("time eslpated %+v\n", time.Since(start))
	c.String(200, "wrote to %d", index)
}

// @Summary Get an object
// @Param id path integer true "Object ID"
// @Produce octet-stream,png,jpeg,gif,plain
// @Success 200 {body} string ""
// @Failure 400 {string} string ""
// @Failure 500 {string} string ""
// @Router /get/{id} [get]
func (as *AspiraServer) get(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		c.String(400, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	data, err := as.getAndWait(ctx, id)
	if err != nil {
		xlog.Logger.Errorf(err.Error())
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
}

func (as *AspiraServer) ServeHTTP() {
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()

	//go tool pprof
	pprof.Register(r)

	/*
		r.Use(gin.LoggerWithConfig(gin.LoggerConfig{
			Output: os.Stdout,
		}))
	*/

	r.Use(gin.Recovery())
	r.Use(logToZap)

	r.POST("/del/:id", as.del)
	r.POST("/put/", as.put)
	r.GET("/get/:id", as.get)

	//http api docs
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	r.GET("/metrics", func(c *gin.Context) {
		cannylsMerics.PrometheusHandler.ServeHTTP(c.Writer, c.Request)
	})

	stringID := fmt.Sprintf("%d", as.node.Id)
	srv := &http.Server{
		Addr:    ":808" + stringID,
		Handler: r,
	}

	go func() {
		defer func() {
			xlog.Logger.Infof("HTTP return")
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

func logToZap(c *gin.Context) {
	start := time.Now()
	// some evil middlewares modify this values
	path := c.Request.URL.Path
	query := c.Request.URL.RawQuery
	c.Next()

	latency := time.Now().Sub(start)

	if len(c.Errors) > 0 {
		// Append error field if this is an erroneous request.
		for _, e := range c.Errors.Errors() {
			xlog.ZapLogger.Error(e)
		}
	} else {
		xlog.ZapLogger.Info(path,
			zap.Int("status", c.Writer.Status()),
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.String("query", query),
			zap.String("ip", c.ClientIP()),
			zap.String("user-agent", c.Request.UserAgent()),
			zap.String("time", start.Format(time.RFC3339)),
			zap.Duration("latency", latency),
		)
	}
}

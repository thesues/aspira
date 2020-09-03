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
// @Param oid path integer true "Object ID"
// @Param gid path int true "Group ID"
// @Success 200 {string} string ""
// @Failure 400 {string} string ""
// @Router /del/{gid}/{oid} [delete]
func (as *AspiraStore) del(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("oid"), 10, 64)
	if err != nil {
		c.String(400, err.Error())
		return
	}
	gid, err := strconv.ParseUint(c.Param("gid"), 10, 64)
	if err != nil {
		c.String(400, err.Error())
	}

	w := as.GetWorker(gid)
	if w == nil {
		c.String(400, "can not find gid %d", gid)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var p aspirapb.AspiraProposal

	p.ProposalType = aspirapb.AspiraProposal_Delete
	p.Key = id

	start := time.Now()
	_, err = w.proposeAndWait(ctx, &p)
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
// @Param   gid path int true "Group ID"
// @Success 200 {string} string ""
// @Failure 400 {string} string ""
// @Router /put/{gid}/ [post]
func (as *AspiraStore) put(c *gin.Context) {
	readFile, header, err := c.Request.FormFile("file")
	if err != nil {
		c.String(400, err.Error())
		return
	}

	gid, err := strconv.ParseUint(c.Param("gid"), 10, 64)
	if err != nil {
		c.String(400, err.Error())
	}

	w := as.GetWorker(gid)

	if w == nil {
		c.String(400, "can not find gid %d", gid)
		return
	}

	if header.Size > (0xFFFF*(512) - 2) {
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
	index, err := w.proposeAndWait(ctx, &p)
	if err != nil {
		xlog.Logger.Errorf(err.Error())
		c.String(400, err.Error())
		return
	}
	xlog.Logger.Infof("time eslpated %+v\n", time.Since(start))
	c.String(200, "wrote to %d", index)
}

// @Summary add new worker
// @Param id  formData integer true "raft ID"
// @Param gid formData integer true "Group ID"
// @Param joinCluster formData string false "remote cluster addr"
// @Success 200 {body} string ""
// @Failure 400 {string} string ""
// @Failure 500 {string} string ""
// @Router /addworker/ [post]
/*
func (as *AspiraStore) add(c *gin.Context) {
	id, err := strconv.ParseUint(c.PostForm("id"), 10, 64)
	if err != nil {
		c.String(400, err.Error())
		return
	}

	gid, err := strconv.ParseUint(c.PostForm("gid"), 10, 64)
	if err != nil {
		c.String(400, err.Error())
	}

	joinCluster := c.PostForm("joinCluster")

	//FIXME
	err = as.startNewWorker(id, gid, as.addr, joinCluster, "")
	if err != nil {
		c.String(500, err.Error())
		return
	}
	c.String(200, "")
	return
}
*/

// @Summary Get an object
// @Param oid path integer true "Object ID"
// @Param gid path integer true "Group ID"
// @Produce octet-stream,png,jpeg,gif,plain
// @Success 200 {body} string ""
// @Failure 400 {string} string ""
// @Failure 500 {string} string ""
// @Router /get/{gid}/{id} [get]
func (as *AspiraStore) get(c *gin.Context) {
	id, err := strconv.ParseUint(c.Param("oid"), 10, 64)
	if err != nil {
		c.String(400, err.Error())
		return
	}

	gid, err := strconv.ParseUint(c.Param("gid"), 10, 64)
	if err != nil {
		c.String(400, err.Error())
	}

	w := as.GetWorker(gid)
	if w == nil {
		c.String(400, "can not find gid %d", gid)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	data, err := w.getAndWait(ctx, id)
	if err != nil {
		xlog.Logger.Info(err.Error())
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

// @Summary List all gid
// @Success 200 {array} uint64
// @Produce json
// @Failure 400 {string} string ""
// @Failure 500 {string} string ""
// @Router /list/ [get]
func (as *AspiraStore) list(c *gin.Context) {
	var gids []uint64
	as.RLock()
	for k := range as.workers {
		gids = append(gids, uint64(k))
	}
	as.RUnlock()
	c.JSON(200, gids)
}

func (as *AspiraStore) ServHTTP() {
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()

	//go tool pprof
	pprof.Register(r)

	r.Use(gin.Recovery())
	//r.Use(logToZap)

	r.DELETE("/del/:gid/:oid", as.del)
	r.POST("/put/:gid/", as.put)
	r.GET("/get/:gid/:oid", as.get)
	r.GET("/list", as.list)
	//r.POST("/addworker", as.add)

	//http api docs
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	r.GET("/metrics", func(c *gin.Context) {
		cannylsMerics.PrometheusHandler.ServeHTTP(c.Writer, c.Request)
	})

	srv := &http.Server{
		Addr:    as.httpAddr,
		Handler: r,
	}

	go func() {
		defer xlog.Logger.Infof("HTTP return")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			panic("http server crashed")
		}
	}()
	as.httpServer = srv

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

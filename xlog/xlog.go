package xlog

import (
	"fmt"

	"go.uber.org/zap"
)

var (
	ZapLogger *zap.Logger
	Logger    *sugaredZap
)

func InitLog(id string) {
	var err error
	cfg := zap.NewDevelopmentConfig()
	fileName := fmt.Sprintf("%s.log", id)
	//cfg.OutputPaths = []string{fileName, os.Stdout.Name()}
	cfg.OutputPaths = []string{fileName}

	cfg.Level.SetLevel(zap.InfoLevel)
	ZapLogger, err = cfg.Build()
	if err != nil {
		panic(err.Error())
	}
	Logger = &sugaredZap{ZapLogger.Sugar()}
}

//etcd raft library requires Warning and Warningf
type sugaredZap struct {
	*zap.SugaredLogger
}

func (logger *sugaredZap) Warning(v ...interface{}) {
	logger.Warn(v...)

}
func (logger *sugaredZap) Warningf(format string, v ...interface{}) {
	logger.Warnf(format, v...)
}

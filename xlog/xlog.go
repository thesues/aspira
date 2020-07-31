package xlog

import (
	"fmt"
	"os"

	"go.uber.org/zap"
)

var (
	ZapLogger *zap.Logger
	Logger    *sugaredZap
)

func InitLog(name string) {
	var err error
	cfg := zap.NewDevelopmentConfig()
	fileName := fmt.Sprintf("%s.log", name)
	cfg.OutputPaths = []string{fileName, os.Stdout.Name()}
	//cfg.OutputPaths = []string{fileName}

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

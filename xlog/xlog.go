package xlog

import (
	"go.uber.org/zap"
)

var (
	ZapLogger *zap.Logger
	Logger    *sugaredZap
)

func InitLog(outputPath []string) {
	var err error
	cfg := zap.NewDevelopmentConfig()
	//cfg.OutputPaths = []string{fileName, os.Stdout.Name()}
	cfg.OutputPaths = outputPath

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

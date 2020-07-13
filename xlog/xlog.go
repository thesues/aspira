package xlog

import (
	"os"

	"go.uber.org/zap"
)

var (
	Logger    *zap.SugaredLogger
	ZapLogger *zap.Logger
)

func InitLog(id uint64) {
	cfg := zap.NewDevelopmentConfig()
	//fileName := fmt.Sprintf("%d.log", id)
	cfg.OutputPaths = []string{os.Stderr.Name()}
	cfg.Level.SetLevel(zap.InfoLevel)
	ZapLogger, err := cfg.Build()
	if err != nil {
		panic(err.Error())
	}
	Logger = ZapLogger.Sugar()
}

/*
func Debug(v ...interface{})                 { Logger.Info(v...) }
func Debugf(format string, v ...interface{}) { Logger.Infof(format, v...) }
func Error(v ...interface{})                 { Logger.Error(v...) }
func Errorf(format string, v ...interface{}) { Logger.Errorf(format, v...) }
func Info(v ...interface{})                  { Logger.Info(v...) }
func Infof(format string, v ...interface{})  { Logger.Infof(format, v...) }
func Warn(v ...interface{})                  { Logger.Warn(v...) }
func Warnf(format string, v ...interface{})  { Logger.Warnf(format, v...) }
func Fatal(v ...interface{})                 { Logger.Fatal(v...) }
func Fatalf(format string, v ...interface{}) { Logger.Fatalf(format, v...) }
func Panic(v ...interface{})                 { Logger.Panic(v...) }
func Panicf(format string, v ...interface{}) { Logger.Panicf(format, v...) }
*/

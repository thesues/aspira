package xlog

import (
	"go.uber.org/zap"
)

var (
	Logger *zap.SugaredLogger
)

func init() {
	cfg := zap.NewDevelopmentConfig()
	cfg.Level.SetLevel(zap.InfoLevel)
	lg, _ := cfg.Build()
	Logger = lg.Sugar()
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

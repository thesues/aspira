package utils

import (
	"log"
	"strings"

	"github.com/golang/glog"
	"github.com/pkg/errors"
	"google.golang.org/grpc/status"
)

func Check(err error) {
	if err != nil {
		glog.Fatalf("%+v", errors.Wrap(err, ""))
	}
}

// ShouldCrash returns true if the error should cause the process to crash.
func ShouldCrash(err error) bool {
	if err == nil {
		return false
	}
	errStr := status.Convert(err).Message()
	return strings.Contains(errStr, "REUSE_RAFTID") ||
		strings.Contains(errStr, "REUSE_ADDR") ||
		strings.Contains(errStr, "NO_ADDR") ||
		strings.Contains(errStr, "ENTERPRISE_LIMIT_REACHED")
}

func AssertTruef(b bool, format string, args ...interface{}) {
	if !b {
		log.Fatalf("%+v", errors.Errorf(format, args...))
	}
}

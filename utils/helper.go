package utils

import (
	"log"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/thesues/aspira/xlog"
	"google.golang.org/grpc/status"
)

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Check(err error) {
	if err != nil {
		xlog.Logger.Fatalf("%+v", errors.Wrap(err, ""))
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

func SplitAndTrim(s string, sep string) []string {
	parts := strings.Split(s, sep)
	for i := 0; i < len(parts); i++ {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func UniqStringArray(array []string) []string {
	sort.Strings(array)
	tail := 0
	for i := 1; i < len(array); i++ {
		if array[i] != array[tail] {
			tail++
			array[tail] = array[i]
		}
	}
	return array[:tail+1]
}

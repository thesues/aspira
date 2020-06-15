package utils

import (
	"github.com/golang/glog"
	"github.com/pkg/errors"
)

func Check(err error) {
	if err != nil {
		glog.Fatalf("%+v", errors.Wrap(err, ""))
	}
}

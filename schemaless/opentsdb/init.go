package opentsdb

import (
	"github.com/taosdata/taosadapter/log"
)

type Result struct {
	SuccessCount int
	FailCount    int
	ErrorList    []error
}

var logger = log.GetLogger("schemaless").WithField("protocol", "opentsdb")

const valueField = "value"

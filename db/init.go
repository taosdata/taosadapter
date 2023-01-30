package db

import (
	"sync"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/async"
	"github.com/taosdata/taosadapter/v3/log"
)

var once = sync.Once{}
var logger = log.GetLogger("db")

func PrepareConnection() {
	once.Do(func() {
		if len(config.Conf.TaosConfigDir) != 0 {
			code := wrapper.TaosOptions(common.TSDB_OPTION_CONFIGDIR, config.Conf.TaosConfigDir)
			if code != 0 {
				errStr := wrapper.TaosErrorStr(nil)
				err := errors.NewError(code, errStr)
				logger.WithError(err).Panic("set taos config file ", config.Conf.TaosConfigDir)
			}
		}
		code := wrapper.TaosOptions(common.TSDB_OPTION_USE_ADAPTER, "true")
		if code != 0 {
			errStr := wrapper.TaosErrorStr(nil)
			err := errors.NewError(code, errStr)
			logger.WithError(err).Panic("set taos option TSDB_OPTION_USE_ADAPTER error")
		}
	})
	async.GlobalAsync = async.NewAsync(async.NewHandlerPool(10000))
}

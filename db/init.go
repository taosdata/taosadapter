package db

import (
	"sync"

	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/async"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/driver/common"
	"github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/driver/wrapper"
	"github.com/taosdata/taosadapter/v3/log"
)

var once = sync.Once{}
var logger = log.GetLogger("OPT")

func PrepareConnection() {
	once.Do(func() {
		if len(config.Conf.TaosConfigDir) != 0 {
			code := wrapper.TaosOptions(common.TSDB_OPTION_CONFIGDIR, config.Conf.TaosConfigDir)
			if code != 0 {
				errStr := syncinterface.TaosErrorStr(nil, logger, log.IsDebug())
				err := errors.NewError(code, errStr)
				logger.WithError(err).Panic("set config file ", config.Conf.TaosConfigDir)
			}
		}
		code := wrapper.TaosOptions(common.TSDB_OPTION_USE_ADAPTER, "true")
		if code != 0 {
			errStr := syncinterface.TaosErrorStr(nil, logger, log.IsDebug())
			err := errors.NewError(code, errStr)
			logger.WithError(err).Panic("set option TSDB_OPTION_USE_ADAPTER error")
		}

		async.GlobalAsync = async.NewAsync(async.NewHandlerPool(10000))
	})
}

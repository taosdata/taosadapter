package recordsql

import (
	"fmt"
	"path/filepath"
	"time"

	rotatelogs "github.com/taosdata/file-rotatelogs/v2"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/version"
)

var globalRotateWriter *rotatelogs.RotateLogs

func getRotateWriter() (*rotatelogs.RotateLogs, error) {
	var err error
	if globalRotateWriter == nil {
		globalRotateWriter, err = rotatelogs.New(
			filepath.Join(config.Conf.Log.Path, fmt.Sprintf("%sadapterSql_%d_%%Y%%m%%d.csv", version.CUS_PROMPT, config.Conf.InstanceID)),
			rotatelogs.WithRotationCount(config.Conf.Log.RotationCount),
			rotatelogs.WithRotationTime(time.Hour*24),
			rotatelogs.WithRotationSize(int64(config.Conf.Log.RotationSize)),
			rotatelogs.WithReservedDiskSize(int64(config.Conf.Log.ReservedDiskSize)),
			rotatelogs.WithRotateGlobPattern(filepath.Join(config.Conf.Log.Path, fmt.Sprintf("%sadapterSql_%d_*.csv*", version.CUS_PROMPT, config.Conf.InstanceID))),
			rotatelogs.WithCompress(config.Conf.Log.Compress),
			rotatelogs.WithCleanLockFile(filepath.Join(config.Conf.Log.Path, fmt.Sprintf(".%sadapterSql_%d_rotate_lock", version.CUS_PROMPT, config.Conf.InstanceID))),
			rotatelogs.ForceNewFile(),
			rotatelogs.WithMaxAge(time.Hour*24*time.Duration(config.Conf.Log.KeepDays)),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize rotate writer: %s", err)
		}
	} else {
		err = globalRotateWriter.Rotate()
		if err != nil {
			return nil, fmt.Errorf("failed to rotate file: %s", err)
		}
	}
	return globalRotateWriter, nil
}

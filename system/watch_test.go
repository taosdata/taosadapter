package system

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/fsnotify/fsnotify"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/config"
)

func TestOnConfigChange(t *testing.T) {
	config.Init()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "config.toml")
	// file not exists
	OnConfigChange(file, fsnotify.Create, logger)
	// create file, no content
	f, err := os.Create(file)
	require.NoError(t, err)
	err = f.Close()
	assert.NoError(t, err)
	OnConfigChange(file, fsnotify.Create, logger)
	// invalid reject content
	err = os.WriteFile(file, []byte("rejectQuerySqlRegex = ['^SELECT * FROM [']"), 0644)
	require.NoError(t, err)
	OnConfigChange(file, fsnotify.Write, logger)
	// valid reject content
	err = os.WriteFile(file, []byte("rejectQuerySqlRegex = ['^SELECT * FROM .*']"), 0644)
	require.NoError(t, err)
	OnConfigChange(file, fsnotify.Write, logger)
	// log level change
	err = os.WriteFile(file, []byte("rejectQuerySqlRegex = ['^SELECT * FROM .*']\n[log]\nlevel = 'debug'"), 0644)
	require.NoError(t, err)
	OnConfigChange(file, fsnotify.Write, logger)
	// wrong log level
	err = os.WriteFile(file, []byte("rejectQuerySqlRegex = ['^SELECT * FROM .*']\n[log]\nlevel = 'dbg'"), 0644)
	require.NoError(t, err)
	OnConfigChange(file, fsnotify.Write, logger)
}

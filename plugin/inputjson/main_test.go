package inputjson

import (
	"os"
	"testing"

	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db"
)

func TestMain(m *testing.M) {
	config.Init()
	db.PrepareConnection()
	os.Exit(m.Run())
}

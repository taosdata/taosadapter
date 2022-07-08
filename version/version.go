package version

import "github.com/taosdata/driver-go/v3/wrapper"

var Version = "0.1.0"

var CommitID = "unknown"

var TaosClientVersion = wrapper.TaosGetClientInfo()

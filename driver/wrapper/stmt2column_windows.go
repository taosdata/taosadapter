//go:build windows

package wrapper

import (
	"fmt"
	"unsafe"
)

func TaosStmt2BindColumnSupported() bool {
	return false
}

func TaosStmt2BindColumnBinary(stmt2 unsafe.Pointer, data []byte) error {
	return fmt.Errorf("taos_stmt2_bind_param_column is not supported on windows build")
}

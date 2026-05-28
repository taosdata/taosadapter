//go:build !windows

package wrapper

/*
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>

#define ADAPTER_STMT2_COLUMN_STACK_CAP 64

static void free_stmt2_column_bindv_columns(TAOS_STMT2_COLUMN_BIND *columns, TAOS_STMT2_COLUMN_BIND *stack_columns) {
  if (columns != NULL && columns != stack_columns) {
    free(columns);
  }
}

static int go_stmt2_bind_column_binary(TAOS_STMT2 *stmt, char *data, char *err_msg) {
  uint32_t *header = (uint32_t *)data;
  uint32_t total_length = header[0];
  uint32_t row_count = header[1];
  uint32_t table_count = header[2];
  uint32_t field_count = header[3];
  uint32_t field_offset = header[4];
  char *data_end = data + total_length;
  char *column_ptr;
  TAOS_STMT2_COLUMN_BINDV bindv;
  TAOS_STMT2_COLUMN_BIND stack_columns[ADAPTER_STMT2_COLUMN_STACK_CAP];
  memset(&bindv, 0, sizeof(bindv));

  if (row_count == 0) {
    snprintf(err_msg, 256, "row count is 0");
    return -1;
  }
  if (field_count == 0) {
    snprintf(err_msg, 256, "field count is 0");
    return -1;
  }
  if (field_offset < 20 || field_offset > total_length) {
    snprintf(err_msg, 256, "field offset out of range, total length: %u, field offset: %u", total_length, field_offset);
    return -1;
  }
  if (field_count > 4096) {
    snprintf(err_msg, 256, "field count exceeds maximum 4096: %u", field_count);
    return -1;
  }

  if (field_count <= ADAPTER_STMT2_COLUMN_STACK_CAP) {
    bindv.columns = stack_columns;
    memset(bindv.columns, 0, field_count * sizeof(TAOS_STMT2_COLUMN_BIND));
  } else {
    bindv.columns = (TAOS_STMT2_COLUMN_BIND *)calloc(field_count, sizeof(TAOS_STMT2_COLUMN_BIND));
  }
  if (bindv.columns == NULL) {
    snprintf(err_msg, 256, "malloc columns error");
    return -1;
  }
  bindv.num_columns = (int)field_count;
  bindv.num_rows = (int)row_count;
  bindv.num_tables = (int)table_count;

  column_ptr = data + field_offset;
  for (uint32_t i = 0; i < field_count; ++i) {
    uint32_t column_length;
    uint32_t num;
    char *current;
    char *column_end;
    char have_length;
    int32_t buffer_length;

    if (column_ptr + 18 > data_end) {
      snprintf(err_msg, 256, "column %u header out of range", i);
      free_stmt2_column_bindv_columns(bindv.columns, stack_columns);
      return -1;
    }

    current = column_ptr;
    column_length = *(uint32_t *)current;
    if (column_length < 18) {
      snprintf(err_msg, 256, "column %u length too short: %u", i, column_length);
      free_stmt2_column_bindv_columns(bindv.columns, stack_columns);
      return -1;
    }
    column_end = current + column_length;
    if (column_end > data_end) {
      snprintf(err_msg, 256, "column %u out of range, total length: %u", i, total_length);
      free_stmt2_column_bindv_columns(bindv.columns, stack_columns);
      return -1;
    }

    bindv.columns[i].buffer_type = *(int32_t *)(current + 4);
    num = *(uint32_t *)(current + 8);
    if (num != row_count) {
      snprintf(err_msg, 256, "column %u row count not match, got: %u, expect: %u", i, num, row_count);
      free_stmt2_column_bindv_columns(bindv.columns, stack_columns);
      return -1;
    }
    if (num > column_length - 13) {
      snprintf(err_msg, 256, "column %u is_null array out of range", i);
      free_stmt2_column_bindv_columns(bindv.columns, stack_columns);
      return -1;
    }
    bindv.columns[i].is_null = current + 12;
    current += 12 + num;

    have_length = *current;
    current += 1;
    if (have_length == 0) {
      bindv.columns[i].length = NULL;
    } else {
      if (current + num * 4 > column_end) {
        snprintf(err_msg, 256, "column %u length array out of range", i);
        free_stmt2_column_bindv_columns(bindv.columns, stack_columns);
        return -1;
      }
      bindv.columns[i].length = (int32_t *)current;
      current += num * 4;
    }

    if (current + 4 > column_end) {
      snprintf(err_msg, 256, "column %u buffer length out of range", i);
      free_stmt2_column_bindv_columns(bindv.columns, stack_columns);
      return -1;
    }
    buffer_length = *(int32_t *)current;
    current += 4;
    if (buffer_length < 0) {
      snprintf(err_msg, 256, "column %u buffer length invalid: %d", i, buffer_length);
      free_stmt2_column_bindv_columns(bindv.columns, stack_columns);
      return -1;
    }
    if (current + buffer_length != column_end) {
      snprintf(err_msg, 256, "column %u data length error", i);
      free_stmt2_column_bindv_columns(bindv.columns, stack_columns);
      return -1;
    }
    bindv.columns[i].buffer = buffer_length == 0 ? NULL : current;
    column_ptr = column_end;
  }

  if (column_ptr != data_end) {
    snprintf(err_msg, 256, "payload has trailing bytes");
    free_stmt2_column_bindv_columns(bindv.columns, stack_columns);
    return -1;
  }

  int code = taos_stmt2_bind_param_column(stmt, &bindv);
  if (code != 0) {
    snprintf(err_msg, 256, "%s", taos_stmt2_error(stmt));
  }
  free_stmt2_column_bindv_columns(bindv.columns, stack_columns);
  return code;
}
*/
import "C"

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"unsafe"

	"github.com/taosdata/taosadapter/v3/driver/common/stmt"
	taosError "github.com/taosdata/taosadapter/v3/driver/errors"
)

func TaosStmt2BindColumnSupported() bool {
	return true
}

func TaosStmt2BindColumnBinary(stmt2 unsafe.Pointer, data []byte) error {
	if len(data) < stmt.Stmt2ColumnDataPosition {
		return fmt.Errorf("data length is less than %d", stmt.Stmt2ColumnDataPosition)
	}
	totalLength := binary.LittleEndian.Uint32(data[stmt.Stmt2ColumnTotalLengthPosition:])
	if totalLength != uint32(len(data)) {
		return fmt.Errorf("total length not match, expect %d, but get %d", len(data), totalLength)
	}
	dataP := unsafe.Pointer(unsafe.SliceData(data))
	var errBuf [256]byte
	errMsg := (*C.char)(unsafe.Pointer(&errBuf[0]))

	code := C.go_stmt2_bind_column_binary(stmt2, (*C.char)(dataP), errMsg)
	runtime.KeepAlive(data)
	if code == -1 {
		return fmt.Errorf("%s", C.GoString(errMsg))
	}
	if code != 0 {
		return taosError.NewError(int(code), C.GoString(errMsg))
	}
	return nil
}

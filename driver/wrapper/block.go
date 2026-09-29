// Copyright (c) 2024 TAOS Data, Inc.
//
// SPDX-License-Identifier: MIT
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package wrapper

/*
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <taos.h>
*/
import "C"
import (
	"unsafe"
)

// TaosFetchRawBlock  int         taos_fetch_raw_block(TAOS_RES *res, int* numOfRows, void** pData);
func TaosFetchRawBlock(result unsafe.Pointer) (int, int, unsafe.Pointer) {
	var cSize int
	size := unsafe.Pointer(&cSize)
	var block unsafe.Pointer
	errCode := int(C.taos_fetch_raw_block(result, (*C.int)(size), &block))
	return cSize, errCode, block
}

// TaosWriteRawBlock DLL_EXPORT int           taos_write_raw_block(TAOS *taos, int numOfRows, char *pData, const char* tbname);
func TaosWriteRawBlock(conn unsafe.Pointer, numOfRows int, pData unsafe.Pointer, tableName string) int {
	cStr := C.CString(tableName)
	defer C.free(unsafe.Pointer(cStr))
	return int(C.taos_write_raw_block(conn, (C.int)(numOfRows), (*C.char)(pData), cStr))
}

// TaosWriteRawBlockWithFields DLL_EXPORT int         taos_write_raw_block_with_fields(TAOS* taos, int rows, char* pData, const char* tbname, TAOS_FIELD *fields, int numFields);
func TaosWriteRawBlockWithFields(conn unsafe.Pointer, numOfRows int, pData unsafe.Pointer, tableName string, fields unsafe.Pointer, numFields int) int {
	cStr := C.CString(tableName)
	defer C.free(unsafe.Pointer(cStr))
	return int(C.taos_write_raw_block_with_fields(conn, (C.int)(numOfRows), (*C.char)(pData), cStr, (*C.struct_taosField)(fields), (C.int)(numFields)))
}

// DLL_EXPORT int taos_write_raw_block_with_reqid(TAOS *taos, int numOfRows, char *pData, const char *tbname, int64_t reqid);
func TaosWriteRawBlockWithReqID(conn unsafe.Pointer, numOfRows int, pData unsafe.Pointer, tableName string, reqID int64) int {
	cStr := C.CString(tableName)
	defer C.free(unsafe.Pointer(cStr))
	return int(C.taos_write_raw_block_with_reqid(conn, (C.int)(numOfRows), (*C.char)(pData), cStr, (C.int64_t)(reqID)))
}

// DLL_EXPORT int taos_write_raw_block_with_fields_with_reqid(TAOS *taos, int rows, char *pData, const char *tbname,TAOS_FIELD *fields, int numFields, int64_t reqid);
func TaosWriteRawBlockWithFieldsWithReqID(conn unsafe.Pointer, numOfRows int, pData unsafe.Pointer, tableName string, fields unsafe.Pointer, numFields int, reqID int64) int {
	cStr := C.CString(tableName)
	defer C.free(unsafe.Pointer(cStr))
	return int(C.taos_write_raw_block_with_fields_with_reqid(conn, (C.int)(numOfRows), (*C.char)(pData), cStr, (*C.struct_taosField)(fields), (C.int)(numFields), (C.int64_t)(reqID)))
}

// Copyright (c) 2023 TAOS Data, Inc.
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

package ws

const actionKey = "action"
const TaosKey = "taos"
const (
	//Deprecated
	//WSWriteRaw                = "write_raw"
	//WSWriteRawBlock           = "write_raw_block"
	//WSWriteRawBlockWithFields = "write_raw_block_with_fields"

	Connect = "conn"
	// websocket
	WSQuery         = "query"
	WSFetch         = "fetch"
	WSFetchBlock    = "fetch_block"
	WSFreeResult    = "free_result"
	WSGetCurrentDB  = "get_current_db"
	WSGetServerInfo = "get_server_info"
	WSNumFields     = "num_fields"

	// schemaless
	SchemalessWrite = "insert"

	// stmt
	STMTInit         = "init"
	STMTPrepare      = "prepare"
	STMTSetTableName = "set_table_name"
	STMTSetTags      = "set_tags"
	STMTBind         = "bind"
	STMTAddBatch     = "add_batch"
	STMTExec         = "exec"
	STMTClose        = "close"
	STMTGetTagFields = "get_tag_fields"
	STMTGetColFields = "get_col_fields"
	STMTUseResult    = "use_result"
	STMTNumParams    = "stmt_num_params"
	STMTGetParam     = "stmt_get_param"

	// stmt2
	STMT2Init     = "stmt2_init"
	STMT2Prepare  = "stmt2_prepare"
	STMT2Exec     = "stmt2_exec"
	STMT2BindExec = "stmt2_bind_exec"
	STMT2Result   = "stmt2_result"
	STMT2Close    = "stmt2_close"

	// options
	OptionsConnection = "options_connection"

	// check_server_status
	CheckServerStatus = "check_server_status"

	GetConnectionInfo = "get_connection_info"
)

const (
	SetTagsMessage            = 1
	BindMessage               = 2
	TMQRawMessage             = 3
	RawBlockMessage           = 4
	RawBlockMessageWithFields = 5
	BinaryQueryMessage        = 6
	FetchRawBlockMessage      = 7
	Stmt2BindMessage          = 9
	ValidateSQL               = 10
	Stmt2BindExecMessage      = 11
)

func getActionString(binaryAction uint64) string {
	switch binaryAction {
	case SetTagsMessage:
		return "set_tags"
	case BindMessage:
		return "bind"
	case TMQRawMessage:
		return "write_raw"
	case RawBlockMessage:
		return "write_raw_block"
	case RawBlockMessageWithFields:
		return "write_raw_block_with_fields"
	case BinaryQueryMessage:
		return "binary_query"
	case FetchRawBlockMessage:
		return "fetch_raw_block"
	case Stmt2BindMessage:
		return "stmt2_bind"
	case ValidateSQL:
		return "validate_sql"
	case Stmt2BindExecMessage:
		return STMT2BindExec
	default:
		return "unknown"
	}
}

const (
	BinaryProtocolVersion1        uint16 = 1
	Stmt2BindProtocolVersion1     uint16 = 1
	Stmt2BindExecProtocolVersion1 uint16 = 1
)

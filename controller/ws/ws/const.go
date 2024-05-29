package ws

const actionKey = "action"
const TaosKey = "taos"
const (
	Connect = "conn"
	// websocket
	WSQuery                   = "query"
	WSFetch                   = "fetch"
	WSFetchBlock              = "fetch_block"
	WSFreeResult              = "free_result"
	WSWriteRaw                = "write_raw"
	WSWriteRawBlock           = "write_raw_block"
	WSWriteRawBlockWithFields = "write_raw_block_with_fields"
	WSGetCurrentDB            = "get_current_db"
	WSGetServerInfo           = "get_server_info"
	WSNumFields               = "num_fields"

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
)

type messageType uint64

const (
	_ messageType = iota
	SetTagsMessage
	BindMessage
	TMQRawMessage
	RawBlockMessage
	RawBlockMessageWithFields
	BinaryQueryMessage
	FetchRawBlockMessage
)

func (m messageType) String() string {
	switch m {
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
	default:
		return "unknown"
	}
}

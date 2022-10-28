package rest

//common

const (
	StartTimeKey = 1
)

//bulk pulling

const TaosSessionKey = "taos"
const (
	ClientVersion   = "version"
	WSConnect       = "conn"
	WSQuery         = "query"
	WSFetch         = "fetch"
	WSFetchBlock    = "fetch_block"
	WSFreeResult    = "free_result"
	WSWriteRaw      = "write_raw"
	WSWriteRawBlock = "write_raw_block"
)

//tmq

const TaosTMQKey = "taos_tmq"
const (
	TMQSubscribe     = "subscribe"
	TMQPoll          = "poll"
	TMQFetch         = "fetch"
	TMQFetchBlock    = "fetch_block"
	TMQFetchRaw      = "fetch_raw"
	TMQFetchJsonMeta = "fetch_json_meta"
	TMQCommit        = "commit"
)

//stmt

const TaosStmtKey = "taos_stmt"
const (
	STMTConnect      = "conn"
	STMTInit         = "init"
	STMTPrepare      = "prepare"
	STMTSetTableName = "set_table_name"
	STMTSetTags      = "set_tags"
	STMTBind         = "bind"
	STMTAddBatch     = "add_batch"
	STMTExec         = "exec"
	STMTClose        = "close"
)

// binaryMessageType
const (
	SetTagsMessage  = 1
	BindMessage     = 2
	TMQRawMessage   = 3
	RawBlockMessage = 4
)

const taosSchemalessKey = "taos_schemaless"
const (
	SchemalessConn  = "conn"
	SchemalessWrite = "insert"
)

package stmt

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
const (
	SetTagsMessage = 1
	BindMessage    = 2
)

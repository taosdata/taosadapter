package tcp

const (
	CmdVersion byte = iota + 1
	CmdConn
	CmdStmt2Init
	CmdStmt2Prepare
	CmdStmt2Bind
	CmdStmt2Execute
	CmdStmt2Close
	CmdTotal
)

var cmdName = [CmdTotal]string{
	CmdVersion:      "version",
	CmdConn:         "conn",
	CmdStmt2Init:    "stmt2_init",
	CmdStmt2Prepare: "stmt2_prepare",
	CmdStmt2Bind:    "stmt2_bind",
	CmdStmt2Execute: "stmt2_execute",
	CmdStmt2Close:   "stmt2_close",
}

package sqltype

import "strings"

const insertStr = "insert"
const selectStr = "select"

type SqlType int

const (
	OtherType  SqlType = -1
	InsertType SqlType = 1
	SelectType SqlType = 2
)

func GetSqlType(sql string) SqlType {
	s := strings.TrimSpace(sql)
	if len(s) < 6 {
		return OtherType
	}
	switch strings.ToLower(s[0:6]) {
	case insertStr:
		return InsertType
	case selectStr:
		return SelectType
	}
	return OtherType
}

package tools

import (
	"strings"

	"github.com/taosdata/taosadapter/common"
	"github.com/taosdata/taosadapter/tools/pool"
)

func RepairName(s string) string {
	result := pool.BytesPoolGet()
	defer pool.BytesPoolPut(result)
	if (s[0] <= 'z' && s[0] >= 'a') || (s[0] <= 'Z' && s[0] >= 'A') || s[0] == '_' {

	} else {
		result.WriteByte('_')
	}
	for i := 0; i < len(s); i++ {
		b := s[i]
		if !checkByte(b) {
			result.WriteByte('_')
		} else {
			result.WriteByte(b)
		}
	}
	r := result.String()
	if IsReservedWords(r) {
		result.Reset()
		result.WriteByte('_')
		result.WriteString(r)
		return result.String()
	} else {
		return r
	}
}

func checkByte(b byte) bool {
	if (b <= 'z' && b >= 'a') || (b <= 'Z' && b >= 'A') || b == '_' || (b <= '9' && b >= '0') {
		return true
	}
	return false
}

func IsReservedWords(s string) bool {
	if _, ok := common.ReservedWords[strings.ToUpper(s)]; ok {
		return true
	}
	return false
}

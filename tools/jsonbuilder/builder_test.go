package jsonbuilder

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBorrowStream(t *testing.T) {
	b := &strings.Builder{}
	s := BorrowStream(b)
	s.WriteString(`"a"`)
	s.Flush()
	assert.Equal(t, `"\"a\""`, b.String())
	ReturnStream(s)
}

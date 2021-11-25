package telnet

import (
	"fmt"
	"strings"
	"sync"

	"github.com/taosdata/taosadapter/tools/pool"
)

type Tags []*Tag

func (t Tags) Len() int {
	return len(t)
}

func (t Tags) Less(i, j int) bool {
	return t[i].Key < t[j].Key
}

func (t Tags) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t Tags) String() string {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	for i, tag := range t {
		b.WriteString(tag.Key)
		b.WriteByte('=')
		b.WriteString(tag.Value)
		if i != len(t)-1 {
			b.WriteByte(' ')
		}
	}
	return b.String()
}

type Tag struct {
	Key   string
	Value string
}

func (t *Tag) unmarshal(s string) error {
	n := strings.IndexByte(s, '=')
	if n < 0 {
		return fmt.Errorf("missing tag value for %q", s)
	}
	t.Key = s[:n]
	t.Value = s[n+1:]
	return nil
}

var tagPool sync.Pool

func tagPoolGet() *Tag {
	return tagPool.Get().(*Tag)
}

func tagPoolPut(t *Tag) {
	tagPool.Put(t)
}

var pointPool sync.Pool

func pointPoolGet() *Point {
	return pointPool.Get().(*Point)
}

func pointPoolPut(point *Point) {
	pointPool.Put(point)
}

func init() {
	tagPool.New = func() interface{} {
		return &Tag{}
	}

	pointPool.New = func() interface{} {
		return &Point{}
	}
}

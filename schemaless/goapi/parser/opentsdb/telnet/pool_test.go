package telnet

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// @author: xftan
// @date: 2021/12/14 15:14
// @description: test parse opentsdb telnet string tag
func TestTags_String(t *testing.T) {
	var dst []*Tag
	tag := tagPoolGet()
	defer tagPoolPut(tag)
	err := tag.unmarshal("k2=v2")
	assert.NoError(t, err)
	dst = append(dst, tag)
	tag2 := tagPoolGet()
	defer tagPoolPut(tag2)
	err = tag2.unmarshal("k1=v1")
	assert.NoError(t, err)
	dst = append(dst, tag2)
	tags := Tags(dst)
	sort.Sort(tags)
	str := tags.String()
	assert.Equal(t, "k1=v1 k2=v2", str)
}

// @author: xftan
// @date: 2021/12/14 15:14
// @description: test parse opentsdb telnet unmarshal
func Test_Unmarshal(t *testing.T) {
	point, err := Unmarshal("put sys.if.bytes.out 1479496100 1.3E3 host=web01 interface=eth0")
	assert.NoError(t, err)
	p := &Point{
		Ts:     time.Unix(1479496100, 0),
		Metric: "sys.if.bytes.out",
		Value:  1.3e3,
		Tags: Tags([]*Tag{
			{
				Key:   "host",
				Value: "web01",
			}, {
				Key:   "interface",
				Value: "eth0",
			},
		}),
	}
	assert.Equal(t, p, point)
	CleanPoint(point)
}

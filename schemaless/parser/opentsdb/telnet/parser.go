package telnet

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/valyala/fastjson/fastfloat"
)

type Point struct {
	Ts     time.Time
	Metric string
	Value  float64
	Tags   Tags
}

func Unmarshal(data string) (*Point, error) {
	p := pointPoolGet()
	p.clean()
	return p.unmarshal(data)
}

func CleanPoint(point *Point) {
	pointPoolPut(point)
}

func (p *Point) unmarshal(s string) (*Point, error) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "put ") {
		return nil, fmt.Errorf("missing `put ` prefix in %q", s)
	}
	//len("put ")
	s = s[4:]
	s = strings.TrimLeft(s, " ")
	n := strings.IndexByte(s, ' ')
	if n < 0 {
		return nil, fmt.Errorf("cannot find whitespace between metric and timestamp in %q", s)
	}

	p.Metric = s[:n]
	if len(p.Metric) == 0 {
		return nil, fmt.Errorf("metric cannot be empty")
	}
	tail := strings.TrimLeft(s[n+1:], " ")
	n = strings.IndexByte(tail, ' ')
	if n < 0 {
		return nil, fmt.Errorf("cannot find whitespace between timestamp and value in %q", s)
	}
	timestamp := tail[:n]

	if len(timestamp) == 14 {
		if timestamp[10] != '.' {
			return nil, fmt.Errorf("cannot parse timestamp from %q: wrong time format", tail[:n])
		}
		second, err := fastfloat.ParseInt64(timestamp[:10])
		if err != nil {
			return nil, fmt.Errorf("cannot parse timestamp from %q: %w", tail[:n], err)
		}
		millisecond, err := fastfloat.ParseInt64(timestamp[11:])
		if err != nil {
			return nil, fmt.Errorf("cannot parse timestamp from %q: %w", tail[:n], err)
		}
		p.Ts = time.Unix(second, millisecond*1e6)
	} else if len(timestamp) == 13 {
		ts, err := fastfloat.ParseInt64(timestamp)
		if err != nil {
			return nil, fmt.Errorf("cannot parse timestamp from %q: %w", tail[:n], err)
		}
		//millisecond
		p.Ts = time.Unix(0, ts*1e6)
	} else if len(timestamp) < 13 {
		//second
		ts, err := fastfloat.ParseInt64(timestamp)
		if err != nil {
			return nil, fmt.Errorf("cannot parse timestamp from %q: %w", tail[:n], err)
		}
		p.Ts = time.Unix(ts, 0)
	} else {
		return nil, fmt.Errorf("cannot parse timestamp from %q: length over 13", tail[:n])
	}
	tail = strings.TrimLeft(tail[n+1:], " ")
	n = strings.IndexByte(tail, ' ')
	if n < 0 {
		return nil, fmt.Errorf("cannot find whitespace between value and the first tag in %q", s)
	}
	v, err := fastfloat.Parse(tail[:n])
	if err != nil {
		return nil, fmt.Errorf("cannot parse value from %q: %w", tail[:n], err)
	}
	p.Value = v
	tags, err := unmarshalTags(tail[n+1:])
	if err != nil {
		return nil, fmt.Errorf("cannot unmarshal tags in %q: %w", s, err)
	}
	sort.Sort(tags)
	p.Tags = tags
	return p, nil
}

func (p *Point) clean() {
	for _, tag := range p.Tags {
		tagPoolPut(tag)
	}
	p.Ts = time.Time{}
	p.Metric = ""
	p.Value = 0
	p.Tags = nil
}

func unmarshalTags(s string) (Tags, error) {
	var dst []*Tag
	for {
		s = strings.TrimLeft(s, " ")
		tag := tagPoolGet()
		if len(s) == 0 {
			return nil, nil
		}
		n := strings.IndexByte(s, ' ')
		if n < 0 {
			// The last tag found
			if err := tag.unmarshal(s); err != nil {
				tagPoolPut(tag)
				return dst, nil
			}
			if len(tag.Key) == 0 || len(tag.Value) == 0 {
				// Skip empty tag
				tagPoolPut(tag)
				return dst, nil
			}
			dst = append(dst, tag)
			return dst, nil
		}
		if err := tag.unmarshal(s[:n]); err != nil {
			for _, t := range dst {
				tagPoolPut(t)
			}
			return nil, err
		}
		s = s[n+1:]
		if len(tag.Key) == 0 || len(tag.Value) == 0 {
			// Skip empty tag
			tagPoolPut(tag)
		}
	}
}

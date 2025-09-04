package innerjson

import (
	"bytes"
	"encoding/json"
	"sync"
)

var encoderPool sync.Pool

type jsonEncoder struct {
	encoder *json.Encoder
	buffer  *bytes.Buffer
}

func getEncoder() *jsonEncoder {
	if v := encoderPool.Get(); v != nil {
		b := v.(*jsonEncoder)
		b.buffer.Reset()
		return b
	}
	buf := &bytes.Buffer{}
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	return &jsonEncoder{
		encoder: enc,
		buffer:  buf,
	}
}

func Marshal(v any) ([]byte, error) {
	encoder := getEncoder()
	defer encoderPool.Put(encoder)
	err := encoder.encoder.Encode(v)
	if err != nil {
		return nil, err
	}
	data := encoder.buffer.Bytes()
	if len(data) > 0 && data[len(data)-1] == '\n' {
		data = data[:len(data)-1]
	}
	// remove the last '\n'
	buf := append([]byte(nil), data...)
	return buf, nil
}

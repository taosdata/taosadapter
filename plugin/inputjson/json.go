package inputjson

import (
	"bytes"
	"encoding/json"
	"sync"
)

var decoderPool sync.Pool

type jsonDecoder struct {
	decoder *json.Decoder
	reader  *bytes.Reader
}

func getDecoder(data []byte) *jsonDecoder {
	if v := decoderPool.Get(); v != nil {
		b := v.(*jsonDecoder)
		b.reader.Reset(data)
		return b
	}
	reader := bytes.NewReader(data)
	dec := json.NewDecoder(reader)
	// UseNumber causes the Decoder to unmarshal a number into an interface{} as a
	// json.Number instead of as a float64.
	dec.UseNumber()
	return &jsonDecoder{
		decoder: dec,
		reader:  reader,
	}
}

func UnMarshal(data []byte, v interface{}) error {
	dec := getDecoder(data)
	defer decoderPool.Put(dec)
	return dec.decoder.Decode(v)
}

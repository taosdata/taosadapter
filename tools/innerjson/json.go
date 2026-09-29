// Copyright (c) 2025 TAOS Data, Inc.
//
// SPDX-License-Identifier: MIT
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

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
		// currently, The json.Encoder.err is only assigned a value when writing to the writer fails.
		//Therefore, even if Encode(v) returns an error, this encoder can still be reused.
		//However, tests need to be added to guard against potential changes in the standard library's behavior.
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

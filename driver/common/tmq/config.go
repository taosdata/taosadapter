// Copyright (c) 2024 TAOS Data, Inc.
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

package tmq

import (
	"fmt"
	"reflect"
)

type ConfigValue interface{}
type ConfigMap map[string]ConfigValue

func (m ConfigMap) Get(key string, defval ConfigValue) (ConfigValue, error) {
	return m.get(key, defval)
}

func (m ConfigMap) get(key string, defval ConfigValue) (ConfigValue, error) {
	v, ok := m[key]
	if !ok {
		return defval, nil
	}

	if defval != nil && reflect.TypeOf(defval) != reflect.TypeOf(v) {
		return nil, fmt.Errorf("%s expects type %T, not %T", key, defval, v)
	}

	return v, nil
}

func (m ConfigMap) Clone() ConfigMap {
	m2 := make(ConfigMap)
	for k, v := range m {
		m2[k] = v
	}
	return m2
}

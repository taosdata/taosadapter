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

package config

import (
	"fmt"
	"regexp"
	"sync"

	"github.com/spf13/viper"
)

type Reject struct {
	sync.RWMutex
	rejectQuerySqlRegex []*regexp.Regexp
}

func (r *Reject) SetValue(v *viper.Viper) error {
	regexpStringSlice := v.GetStringSlice("rejectQuerySqlRegex")
	regexpSlice := make([]*regexp.Regexp, len(regexpStringSlice))
	for i, s := range regexpStringSlice {
		re, err := regexp.Compile(s)
		if err != nil {
			return fmt.Errorf("rejectQuerySqlRegex regexp compile error, string: %s, error: %s", s, err)
		}
		regexpSlice[i] = re
	}
	r.Lock()
	defer r.Unlock()
	r.rejectQuerySqlRegex = regexpSlice
	return nil
}

func (r *Reject) GetRejectQuerySqlRegex() []*regexp.Regexp {
	r.RLock()
	defer r.RUnlock()
	return r.rejectQuerySqlRegex
}

func initRejectConfig(v *viper.Viper) {
	v.SetDefault("rejectQuerySqlRegex", nil)
}

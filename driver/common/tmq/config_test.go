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
	"testing"
)

func TestConfigMap_Get(t *testing.T) {
	t.Parallel()

	config := ConfigMap{
		"key1": "value1",
		"key2": 123,
	}

	t.Run("Existing Key", func(t *testing.T) {
		want := "value1"
		if got, err := config.Get("key1", nil); err != nil || got != want {
			t.Errorf("Get() = %v, want %v (error: %v)", got, want, err)
		}
	})

	t.Run("Type Mismatch", func(t *testing.T) {
		wantErr := fmt.Errorf("key2 expects type string, not int")
		if got, err := config.Get("key2", "default"); err == nil || got != nil || err.Error() != wantErr.Error() {
			t.Errorf("Get() = %v, want error: %v", got, wantErr)
		}
	})

	t.Run("Non-Existing Key with Default Value", func(t *testing.T) {
		want := "default"
		if got, err := config.Get("key3", "default"); err != nil || got != want {
			t.Errorf("Get() = %v, want %v (error: %v)", got, want, err)
		}
	})
}

func TestConfigMap_Clone(t *testing.T) {
	t.Parallel()

	config := ConfigMap{
		"key1": "value1",
		"key2": 123,
	}

	clone := config.Clone()

	if !reflect.DeepEqual(config, clone) {
		t.Errorf("Clone() = %v, want %v", clone, config)
	}
}

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

package inputjson

import (
	"encoding/json"
	"testing"
)

func TestUnmarshal_NormalCase(t *testing.T) {
	type Person struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	data := []byte(`{"name":"Alice","age":30}`)
	var person Person

	err := Unmarshal(data, &person)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if person.Name != "Alice" {
		t.Errorf("Expected name 'Alice', got '%s'", person.Name)
	}
	if person.Age != 30 {
		t.Errorf("Expected age 30, got %d", person.Age)
	}
}

func TestUnmarshal_UseNumber(t *testing.T) {
	data := []byte(`{"number": 12345678901234567890}`)
	var result map[string]interface{}

	err := Unmarshal(data, &result)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	num, ok := result["number"].(json.Number)
	if !ok {
		t.Errorf("Expected json.Number, got %T", result["number"])
	}

	if num.String() != "12345678901234567890" {
		t.Errorf("Expected number '12345678901234567890', got '%s'", num.String())
	}
}

func TestUnmarshal_EmptyData(t *testing.T) {
	var result interface{}
	err := Unmarshal([]byte(""), &result)
	if err == nil {
		t.Error("Expected error for empty data, but got none")
	}
}

func TestUnmarshal_InvalidJSON(t *testing.T) {
	data := []byte(`{"invalid": json}`)
	var result map[string]interface{}

	err := Unmarshal(data, &result)
	if err == nil {
		t.Error("Expected error for invalid JSON, but got none")
	}
}

func TestUnmarshal_NilPointer(t *testing.T) {
	err := Unmarshal([]byte(`{"key": "value"}`), nil)
	if err == nil {
		t.Error("Expected error for nil pointer, but got none")
	}
}

func TestUnmarshal_DifferentTypes(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		target   interface{}
		expected interface{}
	}{
		{
			name:     "string",
			data:     []byte(`"hello"`),
			target:   new(string),
			expected: "hello",
		},
		{
			name:     "number",
			data:     []byte(`42`),
			target:   new(int),
			expected: 42,
		},
		{
			name:     "array",
			data:     []byte(`[1,2,3]`),
			target:   new([]int),
			expected: []int{1, 2, 3},
		},
		{
			name:     "boolean",
			data:     []byte(`true`),
			target:   new(bool),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Unmarshal(tt.data, tt.target)
			if err != nil {
				t.Fatalf("Unmarshal failed: %v", err)
			}

			switch v := tt.target.(type) {
			case *string:
				if *v != tt.expected.(string) {
					t.Errorf("Expected %v, got %v", tt.expected, *v)
				}
			case *int:
				if *v != tt.expected.(int) {
					t.Errorf("Expected %v, got %v", tt.expected, *v)
				}
			case *[]int:
				if len(*v) != len(tt.expected.([]int)) {
					t.Errorf("Expected %v, got %v", tt.expected, *v)
				}
			case *bool:
				if *v != tt.expected.(bool) {
					t.Errorf("Expected %v, got %v", tt.expected, *v)
				}
			}
		})
	}
}

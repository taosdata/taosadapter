// Copyright (c) 2021 TAOS Data, Inc.
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
	"testing"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/stretchr/testify/assert"
)

// @author: xftan
// @date: 2021/12/14 15:00
// @description: test cors config
func TestCorsConfig_GetConfig(t *testing.T) {
	type fields struct {
		AllowAllOrigins  bool
		AllowOrigins     []string
		AllowHeaders     []string
		ExposeHeaders    []string
		AllowCredentials bool
		AllowWebSockets  bool
	}
	tests := []struct {
		name   string
		fields fields
		want   cors.Config
	}{
		{
			name: "AllowAllOrigins",
			fields: fields{
				AllowAllOrigins:  true,
				AllowOrigins:     nil,
				AllowHeaders:     nil,
				ExposeHeaders:    nil,
				AllowCredentials: false,
				AllowWebSockets:  false,
			},
			want: cors.Config{
				AllowAllOrigins:        true,
				AllowOrigins:           nil,
				AllowOriginFunc:        nil,
				AllowMethods:           []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"},
				AllowHeaders:           []string{"Origin", "Content-Length", "Content-Type", "Authorization"},
				AllowCredentials:       false,
				ExposeHeaders:          []string{"Authorization"},
				MaxAge:                 12 * time.Hour,
				AllowWildcard:          true,
				AllowBrowserExtensions: false,
				AllowWebSockets:        false,
				AllowFiles:             false,
			},
		},
		{
			name: "AllowOrigins",
			fields: fields{
				AllowAllOrigins:  false,
				AllowOrigins:     []string{"127.0.0.1"},
				AllowHeaders:     []string{"Self"},
				ExposeHeaders:    nil,
				AllowCredentials: false,
				AllowWebSockets:  false,
			},
			want: cors.Config{
				AllowAllOrigins:        false,
				AllowOrigins:           []string{"127.0.0.1"},
				AllowOriginFunc:        nil,
				AllowMethods:           []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"},
				AllowHeaders:           []string{"Origin", "Content-Length", "Content-Type", "Authorization", "Self"},
				AllowCredentials:       false,
				ExposeHeaders:          []string{"Authorization"},
				MaxAge:                 12 * time.Hour,
				AllowWildcard:          true,
				AllowBrowserExtensions: false,
				AllowWebSockets:        false,
				AllowFiles:             false,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := &CorsConfig{
				AllowAllOrigins:  tt.fields.AllowAllOrigins,
				AllowOrigins:     tt.fields.AllowOrigins,
				AllowHeaders:     tt.fields.AllowHeaders,
				ExposeHeaders:    tt.fields.ExposeHeaders,
				AllowCredentials: tt.fields.AllowCredentials,
				AllowWebSockets:  tt.fields.AllowWebSockets,
			}
			got := conf.GetConfig()
			assert.Equal(t, tt.want, got)

		})
	}
}

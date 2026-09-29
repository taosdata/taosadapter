// Copyright (c) 2022 TAOS Data, Inc.
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

package main

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/taosdata/taosadapter/v3/plugin/prometheus/prompb"
)

func main() {
	worker := 100
	loop := 100000
	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			DisableCompression:    true,
		},
	}
	workerData := make([][][]byte, worker)
	wg := sync.WaitGroup{}
	wg.Add(worker)
	t := time.Now()
	for i := 0; i < worker; i++ {
		i := i
		go func() {
			defer wg.Done()
			data := generateData(fmt.Sprintf("c_%d", i), loop)
			workerData[i] = data
		}()
	}
	wg.Wait()
	fmt.Println(time.Since(t))
	wg.Add(worker)
	for i := 0; i < worker; i++ {
		i := i
		go func() {
			defer wg.Done()
			for _, data := range workerData[i] {
				resp, err := client.Post("http://root:taosdata@127.0.0.1:6041/prometheus/v1/remote_write/test_plugin_prometheus", "", bytes.NewBuffer(data))
				if err != nil {
					panic(err)
				}
				if resp.StatusCode != 202 {
					d, _ := io.ReadAll(resp.Body)
					_ = resp.Body.Close()
					panic(string(d))
				}
				_ = resp.Body.Close()
			}
		}()
	}
	wg.Wait()
	fmt.Println("finish")
}

func generateData(id string, loop int) [][]byte {
	now := time.Now().UnixNano() / 1e6
	reqs := make([][]byte, loop)
	for i := 0; i < loop; i++ {
		var wReq = prompb.WriteRequest{
			Timeseries: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{
							Name:  "id",
							Value: id,
						},
						{
							Name:  "test_k",
							Value: RandStringBytesRmndr(8),
						},
					},
					Samples: []prompb.Sample{
						{
							Value:     rand.Float64(),
							Timestamp: now + int64(i),
						},
					},
				},
			},
		}
		data, err := wReq.Marshal()
		if err != nil {
			panic(err)
		}
		reqs[i] = snappy.Encode(nil, data)
	}
	return reqs
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytesRmndr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

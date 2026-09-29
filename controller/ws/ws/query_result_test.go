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

package ws

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor/recordsql"
	"github.com/taosdata/taosadapter/v3/tools/limiter"
	"github.com/taosdata/taosadapter/v3/tools/testtools"
)

// stmt1 style results do not call into the C driver when freed, so they are safe for unit tests.
func newTestIdleResult() *QueryResult {
	return NewStmt1Result(unsafe.Pointer(new(int)), 0, nil, 0)
}

func setIdleTimeoutForTest(t *testing.T, d time.Duration) {
	t.Helper()
	origin := config.Conf.QueryResult
	config.Conf.QueryResult = &config.QueryResult{IdleTimeout: d}
	t.Cleanup(func() { config.Conf.QueryResult = origin })
}

func TestQueryResultIdleFreeNeverFetched(t *testing.T) {
	setIdleTimeoutForTest(t, time.Hour)
	holder := NewQueryResultHolder()
	logger := log.GetLogger("TQR")
	r := newTestIdleResult()
	id := holder.Add(r)
	// simulate a result created 3 hours ago and never fetched
	r.Lock()
	r.lastFetch = time.Now().Add(-3 * time.Hour)
	r.Unlock()
	holder.FreeResultByIDIfIdle(id, logger)
	assert.Nil(t, holder.Get(id))
	r.Lock()
	assert.Nil(t, r.TaosResult)
	r.Unlock()
}

func TestQueryResultIdleFreeAfterFetch(t *testing.T) {
	setIdleTimeoutForTest(t, time.Hour)
	holder := NewQueryResultHolder()
	logger := log.GetLogger("TQR")
	r := newTestIdleResult()
	id := holder.Add(r)
	r.Lock()
	r.markFetched()
	r.Unlock()
	// fetched just now, not idle yet
	holder.FreeResultByIDIfIdle(id, logger)
	assert.NotNil(t, holder.Get(id))
	// simulate the last fetch happened 3 hours ago
	r.Lock()
	r.lastFetch = time.Now().Add(-3 * time.Hour)
	r.Unlock()
	holder.FreeResultByIDIfIdle(id, logger)
	assert.Nil(t, holder.Get(id))
}

func TestQueryResultIdleFreeFresh(t *testing.T) {
	setIdleTimeoutForTest(t, time.Hour)
	holder := NewQueryResultHolder()
	logger := log.GetLogger("TQR")
	r := newTestIdleResult()
	id := holder.Add(r)
	holder.FreeResultByIDIfIdle(id, logger)
	assert.NotNil(t, holder.Get(id))
	holder.FreeResultByID(id, logger)
	assert.Nil(t, holder.Get(id))
}

func TestQueryResultIdleFreeIdempotent(t *testing.T) {
	setIdleTimeoutForTest(t, time.Hour)
	holder := NewQueryResultHolder()
	logger := log.GetLogger("TQR")
	r := newTestIdleResult()
	id := holder.Add(r)
	holder.FreeResultByID(id, logger)
	// the timer callback fires after the result is already freed: must be a no-op
	holder.FreeResultByIDIfIdle(id, logger)
	assert.Nil(t, holder.Get(id))
}

func TestQueryResultIdleDisabled(t *testing.T) {
	setIdleTimeoutForTest(t, 0)
	holder := NewQueryResultHolder()
	logger := log.GetLogger("TQR")
	r := newTestIdleResult()
	id := holder.Add(r)
	assert.Nil(t, r.timer)
	time.Sleep(80 * time.Millisecond)
	assert.NotNil(t, holder.Get(id))
	holder.FreeResultByID(id, logger)
}

func TestQueryResultIdleConcurrent(t *testing.T) {
	setIdleTimeoutForTest(t, 20*time.Millisecond)
	holder := NewQueryResultHolder()
	logger := log.GetLogger("TQR")
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		r := newTestIdleResult()
		id := holder.Add(r)
		wg.Add(3)
		go func(r *QueryResult) {
			defer wg.Done()
			r.Lock()
			r.markFetched()
			r.Unlock()
		}(r)
		go func(id uint64) {
			defer wg.Done()
			holder.FreeResultByID(id, logger)
		}(id)
		go func(id uint64) {
			defer wg.Done()
			holder.FreeResultByIDIfIdle(id, logger)
		}(id)
	}
	wg.Wait()
	// let the surviving timers fire, then make sure nothing panics and the rest can be freed
	time.Sleep(100 * time.Millisecond)
	holder.FreeAll(logger)
	// the holder must stay functional after the storm: a new result is tracked
	// and released by its own timer
	r := newTestIdleResult()
	id := holder.Add(r)
	assert.Eventually(t, func() bool {
		return holder.Get(id) == nil
	}, 2*time.Second, 10*time.Millisecond)
}

func TestQueryResultIdleTimerFires(t *testing.T) {
	setIdleTimeoutForTest(t, 50*time.Millisecond)
	holder := NewQueryResultHolder()
	r := newTestIdleResult()
	id := holder.Add(r)
	assert.Eventually(t, func() bool {
		return holder.Get(id) == nil
	}, 2*time.Second, 10*time.Millisecond)
}

func TestQueryResultIdleTimerReset(t *testing.T) {
	setIdleTimeoutForTest(t, time.Second)
	holder := NewQueryResultHolder()
	r := newTestIdleResult()
	id := holder.Add(r)
	// fetch at t=600ms, resetting the deadline from t=1s to t=1.6s
	time.Sleep(600 * time.Millisecond)
	r.Lock()
	r.markFetched()
	r.Unlock()
	// at t=1.2s the original deadline has passed but the reset one has not
	time.Sleep(600 * time.Millisecond)
	assert.NotNil(t, holder.Get(id))
	// past the reset deadline, the result must be released
	assert.Eventually(t, func() bool {
		return holder.Get(id) == nil
	}, 3*time.Second, 50*time.Millisecond)
}

// freeLocked handles the record and limiter before the stmt early return, so a stmt1 style
// result can exercise those paths without calling into the C driver.
func TestQueryResultIdleFreeReleasesRecordAndLimiter(t *testing.T) {
	setIdleTimeoutForTest(t, time.Hour)
	holder := NewQueryResultHolder()
	logger := log.GetLogger("TQR")
	l := limiter.NewLimiter(1, 50*time.Millisecond, 1)
	assert.NoError(t, l.Acquire())
	r := newTestIdleResult()
	r.record = &recordsql.SQLRecord{}
	r.limiter = l
	id := holder.Add(r)
	r.Lock()
	r.lastFetch = time.Now().Add(-3 * time.Hour)
	r.Unlock()
	holder.FreeResultByIDIfIdle(id, logger)
	assert.Nil(t, holder.Get(id))
	r.Lock()
	assert.Nil(t, r.record)
	assert.Nil(t, r.TaosResult)
	r.Unlock()
	// the released limiter must accept a new acquire immediately
	assert.NoError(t, l.Acquire())
}

// When the timer callback runs but the result is not idle (a fetch raced with it), the
// consumed AfterFunc timer must be re-armed, otherwise the result would never be reclaimed.
func TestQueryResultIdleRearmAfterConsumedTimer(t *testing.T) {
	setIdleTimeoutForTest(t, 300*time.Millisecond)
	holder := NewQueryResultHolder()
	logger := log.GetLogger("TQR")
	r := newTestIdleResult()
	id := holder.Add(r)
	// simulate the racy state: the timer fired (consumed) while a fetch refreshed lastFetch
	r.Lock()
	r.timer.Stop()
	r.lastFetch = time.Now()
	r.Unlock()
	holder.FreeResultByIDIfIdle(id, logger)
	// not idle yet: the result stays registered, and the re-armed timer releases it later
	assert.NotNil(t, holder.Get(id))
	assert.Eventually(t, func() bool {
		return holder.Get(id) == nil
	}, 2*time.Second, 20*time.Millisecond)
}

// Handler-level coverage for the fetch -> markFetched wiring: a fetch must postpone the idle
// release of the result, otherwise the result is freed at the original deadline and the
// second fetch would fail with "result is nil".
func TestWSFetchResetsIdleTimer(t *testing.T) {
	setIdleTimeoutForTest(t, 2*time.Second)
	s := httptest.NewServer(router)
	defer s.Close()
	code, message := doRestful("drop database if exists test_ws_idle_fetch", "")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("create database if not exists test_ws_idle_fetch", "")
	assert.Equal(t, 0, code, message)
	assert.NoError(t, testtools.EnsureDBCreated("test_ws_idle_fetch"))
	code, message = doRestful("create table if not exists t0 (ts timestamp, v int)", "test_ws_idle_fetch")
	assert.Equal(t, 0, code, message)
	code, message = doRestful("insert into t0 values (now, 1)", "test_ws_idle_fetch")
	assert.Equal(t, 0, code, message)
	defer doRestful("drop database if exists test_ws_idle_fetch", "")

	ws, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(s.URL, "http")+"/ws", nil)
	assert.NoError(t, err)
	defer func() {
		err = ws.Close()
		assert.NoError(t, err)
	}()

	resp, err := doWebSocket(ws, Connect, &connRequest{ReqID: 1, User: "root", Password: "taosdata", DB: "test_ws_idle_fetch"})
	assert.NoError(t, err)
	var connResp connResponse
	assert.NoError(t, json.Unmarshal(resp, &connResp))
	assert.Equal(t, 0, connResp.Code, connResp.Message)

	resp, err = doWebSocket(ws, WSQuery, &queryRequest{ReqID: 2, Sql: "select * from t0"})
	assert.NoError(t, err)
	var queryResp queryResponse
	assert.NoError(t, json.Unmarshal(resp, &queryResp))
	assert.Equal(t, 0, queryResp.Code, queryResp.Message)

	// fetch at t≈1s, moving the idle deadline from t≈2s to t≈3s
	time.Sleep(time.Second)
	resp, err = doWebSocket(ws, WSFetch, &fetchRequest{ReqID: 3, ID: queryResp.ID})
	assert.NoError(t, err)
	var fetchResp fetchResponse
	assert.NoError(t, json.Unmarshal(resp, &fetchResp))
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)

	// at t≈2.2s the original deadline has passed; the result must still be fetchable
	time.Sleep(1200 * time.Millisecond)
	resp, err = doWebSocket(ws, WSFetch, &fetchRequest{ReqID: 4, ID: queryResp.ID})
	assert.NoError(t, err)
	assert.NoError(t, json.Unmarshal(resp, &fetchResp))
	assert.Equal(t, 0, fetchResp.Code, fetchResp.Message)
}

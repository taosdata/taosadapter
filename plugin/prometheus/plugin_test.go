package prometheus

import (
	"bytes"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/config"
	"github.com/taosdata/taosadapter/db"
)

// @author: xftan
// @date: 2021/12/20 14:47
// @description: test prometheus remote_write and remote_read
func TestPrometheus(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	config.Init()
	viper.Set("prometheus.enable", true)
	db.PrepareConnection()

	p := Plugin{}
	router := gin.Default()
	router.Use(func(c *gin.Context) {
		c.Set("currentID", uint32(1))
	})
	err := p.Init(router)
	assert.NoError(t, err)
	err = p.Start()
	assert.NoError(t, err)
	number := rand.Float64()
	defer p.Stop()
	w := httptest.NewRecorder()
	now := time.Now().UnixNano() / 1e6
	var wReq = prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{
						Name:  "testK",
						Value: "testV",
					},
				},
				Samples: []prompb.Sample{
					{
						Value:     number,
						Timestamp: now,
					},
				},
			},
		},
	}
	data, err := proto.Marshal(&wReq)
	assert.NoError(t, err)
	compressed := snappy.Encode(nil, data)
	req, _ := http.NewRequest("POST", "/remote_write/test_plugin_prometheus", bytes.NewBuffer(compressed))
	req.SetBasicAuth("root", "taosdata")
	router.ServeHTTP(w, req)
	assert.Equal(t, 202, w.Code)

	var rReq = prompb.ReadRequest{
		Queries: []*prompb.Query{
			{
				StartTimestampMs: now,
				EndTimestampMs:   now,
				Matchers: []*prompb.LabelMatcher{
					{
						Type:  prompb.LabelMatcher_EQ,
						Name:  "testK",
						Value: "testV",
					},
				},
			},
		},
	}

	rdata, err := proto.Marshal(&rReq)
	assert.NoError(t, err)
	w = httptest.NewRecorder()
	compressedR := snappy.Encode(nil, rdata)
	req, _ = http.NewRequest("POST", "/remote_read/test_plugin_prometheus", bytes.NewBuffer(compressedR))
	req.SetBasicAuth("root", "taosdata")
	router.ServeHTTP(w, req)
	assert.Equal(t, 202, w.Code)
	buf, err := snappy.Decode(nil, w.Body.Bytes())
	assert.NoError(t, err)
	var rr prompb.ReadResponse
	err = proto.Unmarshal(buf, &rr)
	assert.NoError(t, err)
	result := rr.GetResults()
	assert.Equal(t, 1, len(result))
	series := result[0].GetTimeseries()
	assert.Equal(t, 1, len(series))
	labels := series[0].GetLabels()
	assert.Equal(t, 1, len(labels))
	assert.Equal(t, "testK", labels[0].GetName())
	assert.Equal(t, "testV", labels[0].GetValue())
	samples := series[0].GetSamples()
	assert.Equal(t, 1, len(samples))
	assert.Equal(t, number, samples[0].GetValue())
	assert.Equal(t, now, samples[0].GetTimestamp())
}

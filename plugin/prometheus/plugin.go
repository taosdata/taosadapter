package prometheus

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	tErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/plugin"
	prompbWrite "github.com/taosdata/taosadapter/v3/plugin/prometheus/proto/write"
	"github.com/taosdata/taosadapter/v3/tools/pool"
	"github.com/taosdata/taosadapter/v3/tools/web"
)

var logger = log.GetLogger("prometheus")
var bufferPool pool.ByteBufferPool

type Plugin struct {
	conf Config
}

func (p *Plugin) Init(r gin.IRouter) error {
	p.conf.setValue()
	if !p.conf.Enable {
		logger.Info("opentsdb_telnet disabled")
		return nil
	}
	r.Use(plugin.Auth(func(c *gin.Context, code int, err error) {
		c.AbortWithError(code, err)
		return
	}))
	r.POST("remote_read/:db", func(c *gin.Context) {
		if monitor.QueryPaused() {
			c.Header("Retry-After", "120")
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, "query memory exceeds threshold")
			return
		}
	}, p.Read)
	r.POST("remote_write/:db", func(c *gin.Context) {
		if monitor.AllPaused() {
			c.Header("Retry-After", "120")
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, "memory exceeds threshold")
			return
		}
	}, p.Write)
	return nil
}

func (p *Plugin) Start() error {
	return nil
}

func (p *Plugin) Stop() error {
	return nil
}

func (p *Plugin) String() string {
	return "prometheus"
}

func (p *Plugin) Version() string {
	return "v1"
}

func (p *Plugin) Read(c *gin.Context) {
	db := c.Param("db")
	user, password, err := plugin.GetAuth(c)
	if err != nil {
		c.String(http.StatusUnauthorized, err.Error())
		return
	}
	data, err := c.GetRawData()
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	start := time.Now()
	buf, err := snappy.Decode(nil, data)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	logger.Debug("read snappy decode cost:", time.Now().Sub(start))
	var req prompb.ReadRequest
	start = time.Now()
	err = proto.Unmarshal(buf, &req)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	logger.Debug("read protobuf unmarshal cost:", time.Now().Sub(start))
	start = time.Now()
	taosConn, err := commonpool.GetConnection(user, password)
	if err != nil {
		logger.WithError(err).Error("connect taosd error")
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	logger.Debug("read commonpool.GetConnection cost:", time.Now().Sub(start))
	defer func() {
		putErr := taosConn.Put()
		if putErr != nil {
			logger.WithError(putErr).Errorln("taos connect pool put error")
		}
	}()
	resp, err := processRead(taosConn.TaosConnection, &req, db)
	if err != nil {
		taosError, is := err.(*tErrors.TaosError)
		if is {
			web.SetTaosErrorCode(c, int(taosError.Code))
		}
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	start = time.Now()
	respData, err := proto.Marshal(resp)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	logger.Debug("read protobuf marshal cost:", time.Now().Sub(start))
	start = time.Now()
	compressed := snappy.Encode(nil, respData)
	logger.Debug("read snappy encode cost:", time.Now().Sub(start))
	c.Header("Content-Encoding", "snappy")
	c.Data(http.StatusAccepted, "application/x-protobuf", compressed)
}

func (p *Plugin) Write(c *gin.Context) {
	db := c.Param("db")
	ttl := c.Query("ttl")
	ttlI := 0
	var err error
	if len(ttl) > 0 {
		ttlI, err = strconv.Atoi(ttl)
		if err != nil {
			c.String(http.StatusBadRequest, err.Error())
		}
	}
	user, password, err := plugin.GetAuth(c)
	if err != nil {
		c.String(http.StatusUnauthorized, err.Error())
		return
	}
	c.Status(http.StatusAccepted)
	bp := bufferPool.Get()
	_, err = bp.ReadFrom(c.Request.Body)
	if err != nil {
		bufferPool.Put(bp)
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	start := time.Now()
	bb := bufferPool.Get()
	defer bufferPool.Put(bb)
	bb.B, err = snappy.Decode(bb.B[:cap(bb.B)], bp.B)
	bufferPool.Put(bp)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	logger.Debug("snappy decode cost:", time.Now().Sub(start))
	req := prompbWrite.GetWriteRequest()
	defer prompbWrite.PutWriteRequest(req)
	start = time.Now()
	err = req.Unmarshal(bb.B)
	if err != nil {
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	logger.Debug("protobuf unmarshal cost:", time.Now().Sub(start))
	if len(req.Timeseries) == 0 {
		return
	}
	start = time.Now()
	taosConn, err := commonpool.GetConnection(user, password)
	if err != nil {
		logger.WithError(err).Error("connect taosd error")
		c.String(http.StatusBadRequest, err.Error())
		return
	}
	logger.Debug("commonpool.GetConnection cost:", time.Now().Sub(start))
	defer func() {
		putErr := taosConn.Put()
		if putErr != nil {
			logger.WithError(putErr).Errorln("taos connect pool put error")
		}
	}()
	err = processWrite(taosConn.TaosConnection, req, db, ttlI)
	if err != nil {
		taosError, is := err.(*tErrors.TaosError)
		if is {
			web.SetTaosErrorCode(c, int(taosError.Code))
		}
		logger.WithError(err).Error("connect taosd error")
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
}

func init() {
	plugin.Register(&Plugin{})
}

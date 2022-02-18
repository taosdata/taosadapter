package prometheus

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	tErrors "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/taosadapter/db/commonpool"
	"github.com/taosdata/taosadapter/log"
	"github.com/taosdata/taosadapter/monitor"
	"github.com/taosdata/taosadapter/plugin"
	"github.com/taosdata/taosadapter/tools/web"
)

var logger = log.GetLogger("prometheus")

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
		_ = c.AbortWithError(http.StatusUnauthorized, err)
		return
	}
	data, err := c.GetRawData()
	if err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	buf, err := snappy.Decode(nil, data)
	if err != nil {
		_ = c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	var req prompb.ReadRequest
	err = proto.Unmarshal(buf, &req)
	if err != nil {
		_ = c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	taosConn, err := commonpool.GetConnection(user, password)
	if err != nil {
		logger.WithError(err).Error("connect taosd error")
		_ = c.AbortWithError(http.StatusBadRequest, err)
		return
	}
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
		_ = c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	respData, err := proto.Marshal(resp)
	if err != nil {
		_ = c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	compressed := snappy.Encode(nil, respData)
	c.Header("Content-Encoding", "snappy")
	c.Data(http.StatusAccepted, "application/x-protobuf", compressed)
}

func (p *Plugin) Write(c *gin.Context) {
	db := c.Param("db")
	user, password, err := plugin.GetAuth(c)
	if err != nil {
		_ = c.AbortWithError(http.StatusUnauthorized, err)
		return
	}
	c.Status(http.StatusAccepted)
	data, err := c.GetRawData()
	if err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	buf, err := snappy.Decode(nil, data)
	if err != nil {
		_ = c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	var req prompb.WriteRequest
	err = proto.Unmarshal(buf, &req)
	if err != nil {
		_ = c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	if req.GetTimeseries() == nil {
		return
	}
	taosConn, err := commonpool.GetConnection(user, password)
	if err != nil {
		logger.WithError(err).Error("connect taosd error")
		_ = c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	defer func() {
		putErr := taosConn.Put()
		if putErr != nil {
			logger.WithError(putErr).Errorln("taos connect pool put error")
		}
	}()
	err = processWrite(taosConn.TaosConnection, &req, db)
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

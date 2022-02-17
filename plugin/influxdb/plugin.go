package influxdb

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/driver-go/v2/af"
	tErrors "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/taosadapter/db/commonpool"
	"github.com/taosdata/taosadapter/log"
	"github.com/taosdata/taosadapter/monitor"
	"github.com/taosdata/taosadapter/plugin"
	"github.com/taosdata/taosadapter/schemaless/inserter"
	"github.com/taosdata/taosadapter/tools"
	"github.com/taosdata/taosadapter/tools/web"
)

var logger = log.GetLogger("influxdb")

type Influxdb struct {
	conf        Config
	reserveConn *af.Connector
}

func (p *Influxdb) String() string {
	return "influxdb"
}

func (p *Influxdb) Version() string {
	return "v1"
}

func (p *Influxdb) Init(r gin.IRouter) error {
	p.conf.setValue()
	if !p.conf.Enable {
		logger.Info("influxdb disabled")
		return nil
	}
	r.Use(func(c *gin.Context) {
		if monitor.AllPaused() {
			c.Header("Retry-After", "120")
			c.AbortWithStatusJSON(http.StatusServiceUnavailable, "memory exceeds threshold")
			return
		}
	})
	r.POST("write", getAuth, p.write)
	return nil
}

func (p *Influxdb) Start() error {
	if !p.conf.Enable {
		return nil
	}
	return nil
}

func (p *Influxdb) Stop() error {
	if p.reserveConn != nil {
		return p.reserveConn.Close()
	}
	return nil
}

// @Tags influxdb
// @Summary influxdb write
// @Description influxdb write v1 https://docs.influxdata.com/influxdb/v2.0/reference/api/influxdb-1x/write/
// @Accept plain
// @Produce json
// @Param Authorization header string false "basic authorization"
// @Param u query string false "username to authenticate the request"
// @Param p query string false "username to authenticate the request"
// @Param db query string true "the database to write data to"
// @Param precision query string false "the precision of Unix timestamps in the line protocol"
// @Success 204 {string} string "no content"
// @Failure 401 {object} message
// @Failure 400 {object} badRequest
// @Failure 500 {object} message
// @Router /influxdb/v1/write [post]
func (p *Influxdb) write(c *gin.Context) {
	id := web.GetRequestID(c)
	logger := logger.WithField("sessionID", id)
	isDebug := logger.Logger.IsLevelEnabled(logrus.DebugLevel)
	precision := c.Query("precision")
	if len(precision) == 0 {
		precision = "ns"
	}
	user, password, err := plugin.GetAuth(c)
	if err != nil {
		p.commonResponse(c, http.StatusUnauthorized, &message{
			Code:    "forbidden",
			Message: err.Error(),
		})
		return
	}
	db := c.Query("db")
	if len(db) == 0 {
		logger.Errorln("db required")
		p.badRequestResponse(c, &badRequest{
			Code:    "not found",
			Message: "db required",
			Op:      "get db query",
			Err:     "db required",
			Line:    0,
		})
		return
	}
	data, err := c.GetRawData()
	if err != nil {
		logger.WithError(err).Errorln("read line error")
		p.badRequestResponse(c, &badRequest{
			Code:    "internal error",
			Message: "read line error",
			Op:      "read line",
			Err:     err.Error(),
			Line:    0,
		})
		return
	}
	taosConn, err := commonpool.GetConnection(user, password)
	if err != nil {
		logger.WithError(err).Errorln("connect taosd error")
		p.commonResponse(c, http.StatusInternalServerError, &message{Code: "internal error", Message: err.Error()})
		return
	}
	defer func() {
		putErr := taosConn.Put()
		if putErr != nil {
			logger.WithError(putErr).Errorln("taos connect pool put error")
		}
	}()
	conn := taosConn.TaosConnection
	var start time.Time
	if isDebug {
		start = time.Now()
	}
	logger.WithTime(start).Debugln("start insert influxdb:", string(data))
	result, err := inserter.InsertInfluxdb(conn, data, db, precision)
	logger.Debugln("finish insert influxdb cost:", time.Now().Sub(start))
	if err != nil {
		taosError, is := err.(*tErrors.TaosError)
		if is {
			web.SetTaosErrorCode(c, int(taosError.Code))
		}
		logger.WithField("result", result).WithError(err).Errorln("insert line error")
		p.commonResponse(c, http.StatusInternalServerError, &message{Code: "internal error", Message: err.Error()})
		return
	}
	if result.FailCount != 0 {
		logger.WithField("result", result).Errorln("insert line inner error success:", result.SuccessCount, "fail:", result.FailCount, "errors:", strings.Join(result.ErrorList, ","))
		p.commonResponse(c, http.StatusInternalServerError, &message{Code: "internal error", Message: strings.Join(result.ErrorList, ",")})
		return
	}

	c.Status(http.StatusNoContent)
}

type badRequest struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Op      string `json:"op"`
	Err     string `json:"err"`
	Line    int    `json:"line"`
}

type message struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (p *Influxdb) badRequestResponse(c *gin.Context, resp *badRequest) {
	c.JSON(http.StatusBadRequest, resp)
}
func (p *Influxdb) commonResponse(c *gin.Context, code int, resp *message) {
	c.JSON(code, resp)
}

func getAuth(c *gin.Context) {
	auth := c.GetHeader("Authorization")
	if len(auth) != 0 {
		auth = strings.TrimSpace(auth)
		if strings.HasPrefix(auth, "Basic") {
			user, password, err := tools.DecodeBasic(auth[6:])
			if err == nil {
				c.Set(plugin.UserKey, user)
				c.Set(plugin.PasswordKey, password)
			}
		}
	}

	user := c.Query("u")
	password := c.Query("p")
	if len(user) != 0 {
		c.Set(plugin.UserKey, user)
	}
	if len(password) != 0 {
		c.Set(plugin.PasswordKey, password)
	}
}

func init() {
	plugin.Register(&Influxdb{})
}

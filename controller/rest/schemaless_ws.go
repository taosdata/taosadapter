package rest

import (
	"context"
	"encoding/json"
	"sync"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/huskar-t/melody"
	"github.com/sirupsen/logrus"
	tErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/thread"
)

//type SchemalessController struct {
//	schemaless *melody.Melody
//}

type TaosSchemaless struct {
	conn   unsafe.Pointer
	closed bool
	sync.Mutex
}

func NewTaosSchemaless() *TaosSchemaless {
	return &TaosSchemaless{}
}

func (ctl *Restful) InitSchemaless() {
	ctl.schemaless = melody.New()
	ctl.schemaless.Config.MaxMessageSize = 4 * 1024 * 1024

	ctl.schemaless.HandleConnect(func(session *melody.Session) {
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws connect")
		session.Set(taosSchemalessKey, NewTaosSchemaless())
	})

	ctl.schemaless.HandleMessage(func(session *melody.Session, bytes []byte) {
		if ctl.schemaless.IsClosed() {
			return
		}
		ctx := context.WithValue(context.Background(), StartTimeKey, time.Now().UnixNano())
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("get ws message data:", string(bytes))
		var action WSAction
		err := json.Unmarshal(bytes, &action)
		if err != nil {
			logger.WithError(err).Errorln("unmarshal ws request")
			wsError(ctx, session, err, action.Action, 0)
			return
		}
		switch action.Action {
		case ClientVersion:
			session.Write(ctl.wsVersionResp)
		case SchemalessConn:
			var req schemalessConnReq
			if err = json.Unmarshal(action.Args, &req); err != nil {
				logger.WithError(err).Errorln("unmarshal connect request args")
				wsError(ctx, session, err, SchemalessConn, req.ReqID)
				return
			}
			t := session.MustGet(taosSchemalessKey)
			t.(*TaosSchemaless).connect(ctx, session, req)
		case SchemalessWrite:
			var req schemalessWriteReq
			if err = json.Unmarshal(action.Args, &req); err != nil {
				logger.WithError(err).WithField(config.ReqIDKey, req.ReqID).
					Errorln("unmarshal req write request args")
				wsError(ctx, session, err, SchemalessWrite, req.ReqID)
				return
			}
			t := session.MustGet(taosSchemalessKey)
			t.(*TaosSchemaless).insert(ctx, session, req)
		}
	})

	ctl.schemaless.HandleClose(func(session *melody.Session, i int, s string) error {
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws close", i, s)
		t, exist := session.Get(taosSchemalessKey)
		if exist && t != nil {
			t.(*TaosSchemaless).close()
		}
		return nil
	})

	ctl.schemaless.HandleError(func(session *melody.Session, err error) {
		logger := session.MustGet("logger").(*logrus.Entry)
		_, is := err.(*websocket.CloseError)
		if is {
			logger.WithError(err).Debugln("ws close in error")
		} else {
			logger.WithError(err).Errorln("ws error")
		}
		t, exist := session.Get(taosSchemalessKey)
		if exist && t != nil {
			t.(*TaosSchemaless).close()
		}
	})

	ctl.schemaless.HandleDisconnect(func(session *melody.Session) {
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws disconnect")
		t, exist := session.Get(taosSchemalessKey)
		if exist && t != nil {
			t.(*TaosSchemaless).close()
		}
	})
}

func (t *TaosSchemaless) close() {
	t.Lock()
	defer t.Unlock()
	if t.closed {
		return
	}
	t.closed = true
	if t.conn != nil {
		thread.Lock()
		wrapper.TaosClose(t.conn)
		thread.Unlock()
		t.conn = nil
	}
}

// schemalessWs
// @Tags websocket
// @Param Authorization header string true "authorization token"
// @Router /schemaless?db=test&precision=ms
func (ctl *Restful) schemalessWs(c *gin.Context) {
	loggerWithID := logger.WithField("wsType", "schemaless")
	_ = ctl.schemaless.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{"logger": loggerWithID})
}

type schemalessConnReq struct {
	ReqID    uint64 `json:"req_id"`
	User     string `json:"user"`
	Password string `json:"password"`
	DB       string `json:"db"`
}

type schemalessConnResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func (t *TaosSchemaless) connect(ctx context.Context, session *melody.Session, req schemalessConnReq) {
	logger := getLogger(session).WithField("action", SchemalessConn).WithField(config.ReqIDKey, req.ReqID)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.Lock()
	logger.Debugln("get global lock cost:", log.GetLogDuration(isDebug, s))
	defer t.Unlock()
	if t.conn != nil {
		wsSchemalessErrorMsg(ctx, session, 0xffff, "duplicate connections", SchemalessConn, req.ReqID)
		return
	}
	s = log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	conn, err := wrapper.TaosConnect("", req.User, req.Password, req.DB, 0)
	logger.Debugln("connect cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if err != nil {
		wsError(ctx, session, err, SchemalessConn, req.ReqID)
		return
	}
	t.conn = conn
	wsWriteJson(session, &schemalessConnResp{
		Action: SchemalessConn,
		ReqID:  req.ReqID,
		Timing: getDuration(ctx),
	})
}

type schemalessWriteReq struct {
	ReqID     uint64 `json:"req_id"`
	Protocol  int    `json:"protocol"`
	Precision string `json:"precision"`
	TTL       int    `json:"ttl"`
	Data      string `json:"data"`
}

type schemalessResp struct {
	ReqID  uint64 `json:"req_id"`
	Action string `json:"action"`
	Timing int64  `json:"timing"`
}

func (t *TaosSchemaless) insert(ctx context.Context, session *melody.Session, req schemalessWriteReq) {
	if req.Protocol == 0 || len(req.Precision) == 0 {
		wsSchemalessErrorMsg(ctx, session, 0xffff, "args error", SchemalessWrite, req.ReqID)
		return
	}
	if t.conn == nil {
		wsSchemalessErrorMsg(ctx, session, 0xffff, "server not connected", SchemalessWrite, req.ReqID)
		return
	}
	logger := getLogger(session).WithField("action", SchemalessWrite).WithField(config.ReqIDKey, req.ReqID)
	isDebug := log.IsDebug()
	var result unsafe.Pointer
	s := log.GetLogNow(isDebug)
	defer func() {
		if result != nil {
			s = log.GetLogNow(isDebug)
			thread.Lock()
			logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
			s = log.GetLogNow(isDebug)
			wrapper.TaosFreeResult(result)
			logger.Debugln("free result cost:", log.GetLogDuration(isDebug, s))
			thread.Unlock()
		}
	}()
	var err error
	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	_, result = wrapper.TaosSchemalessInsertRawTTLWithReqID(t.conn, req.Data, req.Protocol, req.Precision, req.TTL, int64(req.ReqID))
	logger.Debugln("taos_schemaless_insert_raw_ttl_with_reqid cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()

	if code := wrapper.TaosError(result); code != 0 {
		err = tErrors.NewError(code, wrapper.TaosErrorStr(result))
	}
	if err != nil {
		wsError(ctx, session, err, SchemalessWrite, req.ReqID)
		return
	}
	resp := &schemalessResp{Action: SchemalessWrite, ReqID: req.ReqID, Timing: getDuration(ctx)}
	wsWriteJson(session, resp)
}

type WSSchemalessErrorResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func wsSchemalessErrorMsg(ctx context.Context, session *melody.Session, code int, message string, action string, reqID uint64) {
	b, _ := json.Marshal(&WSSchemalessErrorResp{
		Code:    code & 0xffff,
		Message: message,
		Action:  action,
		ReqID:   reqID,
		Timing:  getDuration(ctx),
	})
	session.Write(b)
}

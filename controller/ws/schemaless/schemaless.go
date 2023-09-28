package schemaless

import (
	"context"
	"encoding/json"
	"net"
	"strings"
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
	"github.com/taosdata/taosadapter/v3/controller"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/thread"
)

type SchemalessController struct {
	schemaless *melody.Melody
}

func NewSchemalessController() *SchemalessController {
	schemaless := melody.New()
	schemaless.UpGrader.EnableCompression = true
	schemaless.Config.MaxMessageSize = 0

	schemaless.HandleConnect(func(session *melody.Session) {
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws connect")
		session.Set(taosSchemalessKey, NewTaosSchemaless(session))
	})

	schemaless.HandleMessage(func(session *melody.Session, bytes []byte) {
		if schemaless.IsClosed() {
			return
		}
		t := session.MustGet(taosSchemalessKey).(*TaosSchemaless)
		if t.closed {
			return
		}
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			ctx := context.WithValue(context.Background(), wstool.StartTimeKey, time.Now().UnixNano())
			logger := session.MustGet("logger").(*logrus.Entry)
			logger.Debugln("get ws message data:", string(bytes))
			var action wstool.WSAction
			err := json.Unmarshal(bytes, &action)
			if err != nil {
				logger.WithError(err).Errorln("unmarshal ws request")
				wstool.WSError(ctx, session, err, action.Action, 0)
				return
			}
			switch action.Action {
			case wstool.ClientVersion:
				session.Write(wstool.VersionResp)
			case SchemalessConn:
				var req schemalessConnReq
				if err = json.Unmarshal(action.Args, &req); err != nil {
					logger.WithError(err).Errorln("unmarshal connect request args")
					wstool.WSError(ctx, session, err, SchemalessConn, req.ReqID)
					return
				}
				t.connect(ctx, session, req)
			case SchemalessWrite:
				var req schemalessWriteReq
				if err = json.Unmarshal(action.Args, &req); err != nil {
					logger.WithError(err).WithField(config.ReqIDKey, req.ReqID).
						Errorln("unmarshal req write request args")
					wstool.WSError(ctx, session, err, SchemalessWrite, req.ReqID)
					return
				}
				t.insert(ctx, session, req)
			}
		}()
	})

	schemaless.HandleClose(func(session *melody.Session, i int, s string) error {
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws close", i, s)
		t, exist := session.Get(taosSchemalessKey)
		if exist && t != nil {
			t.(*TaosSchemaless).close()
		}
		return nil
	})

	schemaless.HandleError(func(session *melody.Session, err error) {
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

	schemaless.HandleDisconnect(func(session *melody.Session) {
		logger := session.MustGet("logger").(*logrus.Entry)
		logger.Debugln("ws disconnect")
		t, exist := session.Get(taosSchemalessKey)
		if exist && t != nil {
			t.(*TaosSchemaless).close()
		}
	})
	return &SchemalessController{schemaless: schemaless}
}

func (s *SchemalessController) Init(ctl gin.IRouter) {
	ctl.GET("rest/schemaless", func(c *gin.Context) {
		logger := log.GetLogger("ws").WithField("wsType", "schemaless")
		_ = s.schemaless.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{"logger": logger})
	})
}

type TaosSchemaless struct {
	conn                unsafe.Pointer
	closed              bool
	exit                chan struct{}
	whitelistChangeChan chan int64
	dropUserNotify      chan struct{}
	session             *melody.Session
	ip                  net.IP
	wg                  sync.WaitGroup
	sync.Mutex
}

func NewTaosSchemaless(session *melody.Session) *TaosSchemaless {
	host, _, _ := net.SplitHostPort(strings.TrimSpace(session.Request.RemoteAddr))
	ipAddr := net.ParseIP(host)
	return &TaosSchemaless{
		exit:                make(chan struct{}),
		whitelistChangeChan: make(chan int64, 1),
		dropUserNotify:      make(chan struct{}, 1),
		session:             session,
		ip:                  ipAddr,
	}
}

func (t *TaosSchemaless) waitSignal() {
	for {
		if t.closed {
			return
		}
		select {
		case <-t.dropUserNotify:
			t.Lock()
			if t.closed {
				t.Unlock()
				return
			}
			logger := wstool.GetLogger(t.session)
			logger.WithField("clientIP", t.session.Request.RemoteAddr).Info("user dropped! close connection!")
			t.session.Close()
			t.Unlock()
			t.close()
			return
		case <-t.whitelistChangeChan:
			t.Lock()
			if t.closed {
				t.Unlock()
				return
			}
			whitelist, err := tool.GetWhitelist(t.conn)
			if err != nil {
				wstool.GetLogger(t.session).WithField("clientIP", t.session.Request.RemoteAddr).WithError(err).Errorln("get whitelist error! close connection!")
				t.session.Close()
				t.Unlock()
				return
			}
			valid := tool.CheckWhitelist(whitelist, t.ip)
			if !valid {
				wstool.GetLogger(t.session).WithField("clientIP", t.session.Request.RemoteAddr).Errorln("ip not in whitelist! close connection!")
				t.session.Close()
				t.Unlock()
				t.close()
				return
			}
			t.Unlock()
		case <-t.exit:
			return
		}
	}
}

func (t *TaosSchemaless) close() {
	t.Lock()
	defer t.Unlock()
	if t.closed {
		return
	}
	t.closed = true
	close(t.exit)
	close(t.whitelistChangeChan)
	close(t.dropUserNotify)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	done := make(chan struct{})
	go func() {
		t.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
	case <-done:
	}
	if t.conn != nil {
		thread.Lock()
		wrapper.TaosClose(t.conn)
		thread.Unlock()
		t.conn = nil
	}
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
	logger := wstool.GetLogger(session).WithField("action", SchemalessConn).WithField(config.ReqIDKey, req.ReqID)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	t.Lock()
	logger.Debugln("get global lock cost:", log.GetLogDuration(isDebug, s))
	defer t.Unlock()
	if t.closed {
		return
	}
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
		wstool.WSError(ctx, session, err, SchemalessConn, req.ReqID)
		return
	}
	whitelist, err := tool.GetWhitelist(conn)
	if err != nil {
		thread.Lock()
		wrapper.TaosClose(conn)
		thread.Unlock()
		wstool.WSError(ctx, session, err, SchemalessConn, req.ReqID)
		return
	}
	valid := tool.CheckWhitelist(whitelist, t.ip)
	if !valid {
		thread.Lock()
		wrapper.TaosClose(conn)
		thread.Unlock()
		wstool.WSErrorMsg(ctx, session, 0xffff, "whitelist prohibits current IP access", SchemalessConn, req.ReqID)
		return
	}
	err = tool.RegisterChangeWhitelist(conn, t.whitelistChangeChan)
	if err != nil {
		thread.Lock()
		wrapper.TaosClose(conn)
		thread.Unlock()
		wstool.WSError(ctx, session, err, SchemalessConn, req.ReqID)
		return
	}
	err = tool.RegisterDropUser(conn, t.dropUserNotify)
	if err != nil {
		thread.Lock()
		wrapper.TaosClose(conn)
		thread.Unlock()
		wstool.WSError(ctx, session, err, SchemalessConn, req.ReqID)
		return
	}
	t.conn = conn
	go t.waitSignal()
	wstool.WSWriteJson(session, &schemalessConnResp{
		Action: SchemalessConn,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
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
	Code    int    `json:"code"`
	Message string `json:"message"`
	ReqID   uint64 `json:"req_id"`
	Action  string `json:"action"`
	Timing  int64  `json:"timing"`
}

func (t *TaosSchemaless) insert(ctx context.Context, session *melody.Session, req schemalessWriteReq) {
	if req.Protocol == 0 {
		wsSchemalessErrorMsg(ctx, session, 0xffff, "args error", SchemalessWrite, req.ReqID)
		return
	}
	if t.conn == nil {
		wsSchemalessErrorMsg(ctx, session, 0xffff, "server not connected", SchemalessWrite, req.ReqID)
		return
	}
	logger := wstool.GetLogger(session).WithField("action", SchemalessWrite).WithField(config.ReqIDKey, req.ReqID)
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
		wstool.WSError(ctx, session, err, SchemalessWrite, req.ReqID)
		return
	}
	resp := &schemalessResp{Action: SchemalessWrite, ReqID: req.ReqID, Timing: wstool.GetDuration(ctx)}
	wstool.WSWriteJson(session, resp)
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
		Timing:  wstool.GetDuration(ctx),
	})
	session.Write(b)
}

func init() {
	c := NewSchemalessController()
	controller.AddController(c)
}

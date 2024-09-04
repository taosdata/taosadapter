package schemaless

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/huskar-t/melody"
	"github.com/sirupsen/logrus"
	tErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/driver-go/v3/wrapper/cgo"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/controller"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	"github.com/taosdata/taosadapter/v3/db/tool"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/generator"
	"github.com/taosdata/taosadapter/v3/tools/iptool"
)

type SchemalessController struct {
	schemaless *melody.Melody
}

func NewSchemalessController() *SchemalessController {
	schemaless := melody.New()
	schemaless.UpGrader.EnableCompression = true
	schemaless.Config.MaxMessageSize = 0

	schemaless.HandleConnect(func(session *melody.Session) {
		logger := wstool.GetLogger(session)
		logger.Debug("ws connect")
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
			logger := wstool.GetLogger(session)
			logger.Debugf("get ws message data:%s", bytes)
			var action wstool.WSAction
			err := json.Unmarshal(bytes, &action)
			if err != nil {
				logger.Errorf("unmarshal ws request error, err:%s", err)
				wstool.WSError(ctx, session, err, action.Action, 0)
				return
			}
			switch action.Action {
			case wstool.ClientVersion:
				session.Write(wstool.VersionResp)
			case SchemalessConn:
				var req schemalessConnReq
				if err = json.Unmarshal(action.Args, &req); err != nil {
					logger.Errorf("unmarshal connect args, err:%s, args:%s", err, action.Args)
					wstool.WSError(ctx, session, err, SchemalessConn, req.ReqID)
					return
				}
				t.connect(ctx, session, req)
			case SchemalessWrite:
				var req schemalessWriteReq
				if err = json.Unmarshal(action.Args, &req); err != nil {
					logger.Errorf("unmarshal schemaless insert args, err:%s, args:%s", err, action.Args)
					wstool.WSError(ctx, session, err, SchemalessWrite, req.ReqID)
					return
				}
				t.insert(ctx, session, req)
			default:
				logger.Errorf("unknown action:%s", action.Action)
				return
			}
		}()
	})

	schemaless.HandleClose(func(session *melody.Session, i int, s string) error {
		logger := wstool.GetLogger(session)
		logger.Debugf("ws close, code:%d, msg %s", i, s)
		t, exist := session.Get(taosSchemalessKey)
		if exist && t != nil {
			t.(*TaosSchemaless).Close(logger)
		}
		return nil
	})

	schemaless.HandleError(func(session *melody.Session, err error) {
		wstool.LogWSError(session, err)
		logger := wstool.GetLogger(session)
		t, exist := session.Get(taosSchemalessKey)
		if exist && t != nil {
			t.(*TaosSchemaless).Close(logger)
		}
	})

	schemaless.HandleDisconnect(func(session *melody.Session) {
		logger := wstool.GetLogger(session)
		logger.Debug("ws disconnect")
		t, exist := session.Get(taosSchemalessKey)
		if exist && t != nil {
			t.(*TaosSchemaless).Close(logger)
		}
	})
	return &SchemalessController{schemaless: schemaless}
}

func (s *SchemalessController) Init(ctl gin.IRouter) {
	ctl.GET("rest/schemaless", func(c *gin.Context) {
		sessionID := generator.GetSessionID()
		logger := log.GetLogger("SML").WithFields(logrus.Fields{
			config.SessionIDKey: sessionID})
		_ = s.schemaless.HandleRequestWithKeys(c.Writer, c.Request, map[string]interface{}{"logger": logger})
	})
}

type TaosSchemaless struct {
	conn                  unsafe.Pointer
	logger                *logrus.Entry
	closed                bool
	exit                  chan struct{}
	whitelistChangeChan   chan int64
	dropUserChan          chan struct{}
	session               *melody.Session
	ip                    net.IP
	ipStr                 string
	wg                    sync.WaitGroup
	whitelistChangeHandle cgo.Handle
	dropUserHandle        cgo.Handle
	sync.Mutex
}

func NewTaosSchemaless(session *melody.Session) *TaosSchemaless {
	logger := wstool.GetLogger(session)
	ipAddr := iptool.GetRealIP(session.Request)
	whitelistChangeChan, whitelistChangeHandle := tool.GetRegisterChangeWhiteListHandle()
	dropUserChan, dropUserHandle := tool.GetRegisterDropUserHandle()
	return &TaosSchemaless{
		exit:                  make(chan struct{}),
		whitelistChangeChan:   whitelistChangeChan,
		whitelistChangeHandle: whitelistChangeHandle,
		dropUserChan:          dropUserChan,
		dropUserHandle:        dropUserHandle,
		session:               session,
		ip:                    ipAddr,
		ipStr:                 ipAddr.String(),
		logger:                logger,
	}
}

func (t *TaosSchemaless) waitSignal(logger *logrus.Entry) {
	defer func() {
		logger.Trace("exit wait signal")
		tool.PutRegisterChangeWhiteListHandle(t.whitelistChangeHandle)
		tool.PutRegisterDropUserHandle(t.dropUserHandle)
	}()
	for {
		select {
		case <-t.dropUserChan:
			logger.Info("get drop user signal")
			isDebug := log.IsDebug()
			t.lock(logger, isDebug)
			if t.closed {
				logger.Trace("server closed")
				t.Unlock()
				return
			}
			logger.Info("user dropped! close connection!")
			s := log.GetLogNow(isDebug)
			t.session.Close()
			logger.Debugf("close session cost:%s", log.GetLogDuration(isDebug, s))
			t.Unlock()
			s = log.GetLogNow(isDebug)
			t.Close(logger)
			logger.Debugf("close handler cost:%s", log.GetLogDuration(isDebug, s))
			return
		case <-t.whitelistChangeChan:
			logger.Info("get whitelist change signal")
			isDebug := log.IsDebug()
			t.lock(logger, isDebug)
			if t.closed {
				logger.Trace("server closed")
				t.Unlock()
				return
			}
			logger.Trace("get whitelist")
			s := log.GetLogNow(isDebug)
			whitelist, err := tool.GetWhitelist(t.conn)
			logger.Debugf("get whitelist cost:%s", log.GetLogDuration(isDebug, s))
			if err != nil {
				logger.Errorf("get whitelist error, close connection, err:%s", err)
				wstool.GetLogger(t.session).WithField("ip", t.ipStr).WithError(err).Errorln("get whitelist error! close connection!")
				s = log.GetLogNow(isDebug)
				t.session.Close()
				logger.Debugf("close session cost:%s", log.GetLogDuration(isDebug, s))
				t.Unlock()
				s = log.GetLogNow(isDebug)
				t.Close(t.logger)
				logger.Debugf("close handler cost:%s", log.GetLogDuration(isDebug, s))
				return
			}
			logger.Tracef("check whitelist, ip:%s, whitelist:%s", t.ipStr, tool.IpNetSliceToString(whitelist))
			valid := tool.CheckWhitelist(whitelist, t.ip)
			if !valid {
				logger.Errorf("ip not in whitelist, close connection, ip:%s, whitelist:%s", t.ipStr, tool.IpNetSliceToString(whitelist))
				s = log.GetLogNow(isDebug)
				t.session.Close()
				logger.Debugf("close session cost:%s", log.GetLogDuration(isDebug, s))
				t.Unlock()
				s = log.GetLogNow(isDebug)
				t.Close(logger)
				logger.Debugf("close handler cost:%s", log.GetLogDuration(isDebug, s))
				return
			}
			t.Unlock()
		case <-t.exit:
			return
		}
	}
}

func (t *TaosSchemaless) lock(logger *logrus.Entry, isDebug bool) {
	s := log.GetLogNow(isDebug)
	logger.Trace("get handler lock")
	t.Lock()
	logger.Debugf("get handler lock cost:%s", log.GetLogDuration(isDebug, s))
}

func (t *TaosSchemaless) Close(logger *logrus.Entry) {
	t.lock(logger, log.IsDebug())
	defer t.Unlock()
	if t.closed {
		return
	}
	logger.Info("schemaless close")
	t.closed = true
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	done := make(chan struct{})
	go func() {
		t.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		logger.Error("wait for all goroutines to exit timeout")
	case <-done:
		logger.Debug("all goroutines exit")
	}
	if t.conn != nil {
		syncinterface.TaosClose(t.conn, logger, log.IsDebug())
		t.conn = nil
	}
	close(t.exit)
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
	action := SchemalessConn
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("connect request:%+v", req)
	isDebug := log.IsDebug()
	t.lock(logger, isDebug)
	defer t.Unlock()
	if t.closed {
		logger.Trace("server closed")
		return
	}
	if t.conn != nil {
		logger.Errorf("duplicate connections")
		wsSchemalessErrorMsg(ctx, session, 0xffff, "duplicate connections", action, req.ReqID)
		return
	}
	conn, err := syncinterface.TaosConnect("", req.User, req.Password, req.DB, 0, logger, isDebug)
	if err != nil {
		logger.Errorf("connect error, err:%s", err)
		wstool.WSError(ctx, session, err, action, req.ReqID)
		return
	}
	s := log.GetLogNow(isDebug)
	whitelist, err := tool.GetWhitelist(conn)
	logger.Debugf("get whitelist cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.Errorf("get whitelist error, close connection, err:%s", err)
		syncinterface.TaosClose(conn, t.logger, isDebug)
		wstool.WSError(ctx, session, err, action, req.ReqID)
		return
	}
	logger.Tracef("check whitelist, ip:%s, whitelist:%s", t.ipStr, tool.IpNetSliceToString(whitelist))
	valid := tool.CheckWhitelist(whitelist, t.ip)
	if !valid {
		logger.Errorf("ip not in whitelist, close connection, ip:%s, whitelist:%s", t.ipStr, tool.IpNetSliceToString(whitelist))
		syncinterface.TaosClose(conn, t.logger, isDebug)
		wstool.WSErrorMsg(ctx, session, 0xffff, "whitelist prohibits current IP access", action, req.ReqID)
		return
	}
	logger.Trace("register change whitelist")
	err = tool.RegisterChangeWhitelist(conn, t.whitelistChangeHandle)
	if err != nil {
		logger.Errorf("register change whitelist error:%s", err)
		syncinterface.TaosClose(conn, t.logger, isDebug)
		wstool.WSError(ctx, session, err, action, req.ReqID)
		return
	}
	logger.Trace("register drop user")
	err = tool.RegisterDropUser(conn, t.dropUserHandle)
	if err != nil {
		logger.Errorf("register drop user error:%s", err)
		syncinterface.TaosClose(conn, t.logger, isDebug)
		wstool.WSError(ctx, session, err, action, req.ReqID)
		return
	}
	t.conn = conn
	logger.Trace("start to wait signal")
	go t.waitSignal(t.logger)
	wstool.WSWriteJson(session, logger, &schemalessConnResp{
		Action: action,
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
	Code         int    `json:"code"`
	Message      string `json:"message"`
	ReqID        uint64 `json:"req_id"`
	Action       string `json:"action"`
	Timing       int64  `json:"timing"`
	AffectedRows int    `json:"affected_rows"`
	TotalRows    int32  `json:"total_rows"`
}

func (t *TaosSchemaless) insert(ctx context.Context, session *melody.Session, req schemalessWriteReq) {
	action := SchemalessConn
	logger := t.logger.WithField("action", action).WithField(config.ReqIDKey, req.ReqID)
	logger.Tracef("schemaless insert request:%+v", req)
	isDebug := log.IsDebug()
	if req.Protocol == 0 {
		logger.Errorf("args error, protocol is 0")
		wsSchemalessErrorMsg(ctx, session, 0xffff, "args error", action, req.ReqID)
		return
	}
	if t.conn == nil {
		logger.Errorf("server not connected")
		wsSchemalessErrorMsg(ctx, session, 0xffff, "server not connected", action, req.ReqID)
		return
	}
	var result unsafe.Pointer
	defer func() {
		if result != nil {
			syncinterface.FreeResult(result, logger, isDebug)
		}
	}()
	var err error
	var totalRows int32
	var affectedRows int
	totalRows, result = syncinterface.TaosSchemalessInsertRawTTLWithReqID(
		t.conn,
		req.Data,
		req.Protocol,
		req.Precision,
		req.TTL,
		int64(req.ReqID),
		logger,
		isDebug,
	)

	if code := wrapper.TaosError(result); code != 0 {
		err = tErrors.NewError(code, wrapper.TaosErrorStr(result))
	}
	if err != nil {
		logger.Errorf("insert error, err:%s", err)
		wstool.WSError(ctx, session, err, action, req.ReqID)
		return
	}
	affectedRows = wrapper.TaosAffectedRows(result)
	logger.Tracef("insert success, affected rows:%d, total rows:%d", affectedRows, totalRows)
	resp := &schemalessResp{
		ReqID:        req.ReqID,
		Action:       action,
		Timing:       wstool.GetDuration(ctx),
		AffectedRows: affectedRows,
		TotalRows:    totalRows,
	}
	wstool.WSWriteJson(session, logger, resp)
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

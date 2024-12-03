package ws

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/db/syncinterface"
	errors2 "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/tools/melody"
)

type getCurrentDBRequest struct {
	ReqID uint64 `json:"req_id"`
}

type getCurrentDBResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	DB      string `json:"db"`
}

func (h *messageHandler) getCurrentDB(ctx context.Context, session *melody.Session, action string, req getCurrentDBRequest, logger *logrus.Entry, isDebug bool) {
	logger.Tracef("get current db")
	db, err := syncinterface.TaosGetCurrentDB(h.conn, logger, isDebug)
	if err != nil {
		logger.Errorf("get current db error, err:%s", err)
		taosErr := err.(*errors2.TaosError)
		commonErrorResponse(ctx, session, logger, action, req.ReqID, int(taosErr.Code), taosErr.Error())
		return
	}
	resp := &getCurrentDBResponse{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
		DB:     db,
	}
	wstool.WSWriteJson(session, logger, resp)
}

type getServerInfoRequest struct {
	ReqID uint64 `json:"req_id"`
}

type getServerInfoResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	Info    string `json:"info"`
}

func (h *messageHandler) getServerInfo(ctx context.Context, session *melody.Session, action string, req getServerInfoRequest, logger *logrus.Entry, isDebug bool) {
	logger.Trace("get server info")
	serverInfo := syncinterface.TaosGetServerInfo(h.conn, logger, isDebug)
	resp := &getServerInfoResponse{
		Action: action,
		ReqID:  req.ReqID,
		Timing: wstool.GetDuration(ctx),
		Info:   serverInfo,
	}
	wstool.WSWriteJson(session, logger, resp)
}

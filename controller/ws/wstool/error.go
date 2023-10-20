package wstool

import (
	"context"
	"encoding/json"

	"github.com/huskar-t/melody"
	tErrors "github.com/taosdata/driver-go/v3/errors"
)

type WSErrorResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
}

func WSErrorMsg(ctx context.Context, session *melody.Session, code int, message string, action string, reqID uint64) {
	b, _ := json.Marshal(&WSErrorResp{
		Code:    code & 0xffff,
		Message: message,
		Action:  action,
		ReqID:   reqID,
		Timing:  GetDuration(ctx),
	})
	session.Write(b)
}
func WSError(ctx context.Context, session *melody.Session, err error, action string, reqID uint64) {
	e, is := err.(*tErrors.TaosError)
	if is {
		WSErrorMsg(ctx, session, int(e.Code)&0xffff, e.ErrStr, action, reqID)
	} else {
		WSErrorMsg(ctx, session, 0xffff, err.Error(), action, reqID)
	}
}

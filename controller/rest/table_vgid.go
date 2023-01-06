package rest

import (
	"net/http"

	"github.com/gin-gonic/gin"
	tErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/thread"
)

func (*Restful) tableVgID(c *gin.Context) {
	db := c.Param("db")
	var tables []string
	err := c.ShouldBindJSON(&tables)
	if err != nil {
		ErrorResponseWithStatusMsg(c, http.StatusBadRequest, 0xffff, err.Error())
		return
	}
	if len(db) == 0 || len(tables) == 0 {
		ErrorResponseWithStatusMsg(c, http.StatusBadRequest, 0xffff, "illegal params")
		return
	}

	user := c.MustGet(UserKey).(string)
	password := c.MustGet(PasswordKey).(string)
	isDebug := log.IsDebug()
	s := log.GetLogNow(isDebug)
	taosConn, err := commonpool.GetConnection(user, password)
	logger.Debugln("taos connect cost:", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.WithError(err).Error("connect taosd error")
		if tError, is := err.(*tErrors.TaosError); is {
			if isDbAuthFail(tError.Code) {
				ErrorResponseWithStatusMsg(c, http.StatusUnauthorized, int(tError.Code), tError.ErrStr)
				return
			}
			ErrorResponseWithMsg(c, int(tError.Code), tError.ErrStr)
			return
		}
		ErrorResponseWithMsg(c, 0xffff, err.Error())
		return
	}
	defer func() {
		putErr := taosConn.Put()
		if putErr != nil {
			logger.WithError(putErr).Errorln("taos connect pool put error")
		}
	}()
	s = log.GetLogNow(isDebug)
	thread.Lock()
	logger.Debugln("get thread lock cost:", log.GetLogDuration(isDebug, s))
	s = log.GetLogNow(isDebug)
	vgIDs, code := wrapper.TaosGetTablesVgID(taosConn.TaosConnection, db, tables)
	logger.Debugln("taos_get_tables_vgId cost:", log.GetLogDuration(isDebug, s))
	thread.Unlock()
	if code != 0 {
		ErrorResponseWithStatusMsg(c, http.StatusInternalServerError, code, wrapper.TaosErrorStr(nil))
		return
	}

	c.JSON(http.StatusOK, tableVgIDResp{Code: 0, VgIDs: vgIDs})
}

type tableVgIDResp struct {
	Code  int   `json:"code"`
	VgIDs []int `json:"vgIDs"`
}

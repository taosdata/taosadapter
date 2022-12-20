package rest

import (
	"net/http"

	"github.com/gin-gonic/gin"
	tErrors "github.com/taosdata/driver-go/v3/errors"
	"github.com/taosdata/driver-go/v3/wrapper"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
)

// tableVgID
// @description get table vg_id
// @Router rest/vgid?db={db}&table={table}
func (*Restful) tableVgID(c *gin.Context) {
	db := c.Query("db")
	table := c.Query("table")

	if len(db) == 0 || len(table) == 0 {
		ErrorResponseWithStatusMsg(c, http.StatusBadRequest, 0xffff, "illegal params")
		return
	}

	user := c.MustGet(UserKey).(string)
	password := c.MustGet(PasswordKey).(string)

	taosConn, err := commonpool.GetConnection(user, password)
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

	vgID, code := wrapper.TaosGetTableVgID(taosConn.TaosConnection, db, table)
	if code != 0 {
		ErrorResponseWithStatusMsg(c, http.StatusInternalServerError, code, wrapper.TaosErrorStr(nil))
		return
	}

	c.JSON(http.StatusOK, tableVgIDResp{Code: 0, VgID: vgID})
}

type tableVgIDResp struct {
	Code int `json:"code"`
	VgID int `json:"vgID"`
}

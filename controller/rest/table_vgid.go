package rest

import (
	"encoding/json"
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

	j, _ := json.Marshal(map[string]int{"vgID": vgID})

	w := c.Writer
	w.WriteHeader(http.StatusOK)
	c.Header("Content-Type", "application/json; charset=utf-8")
	w.Write(j)
	w.Flush()
}

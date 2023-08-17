package rest

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/tools/web"
)

func UnAuthResponse(c *gin.Context, code int) {
	badResponse(c, http.StatusUnauthorized, code)
}

func BadRequestResponse(c *gin.Context, code int) {
	badResponse(c, http.StatusBadRequest, code)
}

func badResponse(c *gin.Context, httpCode int, code int) {
	errStr, exist := httperror.ErrorMsgMap[code]
	if !exist {
		errStr = "unknown error"
	}
	errorResp(c, httpCode, code, errStr)
}

func BadRequestResponseWithMsg(c *gin.Context, code int, msg string) {
	errorResp(c, http.StatusBadRequest, code, msg)
}

func getErrorHttpStatus(errCode int32) int {
	if config.Conf.HttpCodeServerError {
		httpCode, exist := errorStatusMap[errCode]
		if exist {
			return httpCode
		}
		return http.StatusInternalServerError
	}
	return http.StatusOK
}

var errorStatusMap = map[int32]int{
	//400
	httperror.TSDB_CODE_TSC_SQL_SYNTAX_ERROR:        http.StatusBadRequest,
	httperror.TSDB_CODE_TSC_LINE_SYNTAX_ERROR:       http.StatusBadRequest,
	httperror.TSDB_CODE_PAR_SYNTAX_ERROR:            http.StatusBadRequest,
	httperror.TSDB_CODE_TDB_TIMESTAMP_OUT_OF_RANGE:  http.StatusBadRequest,
	httperror.TSDB_CODE_TSC_VALUE_OUT_OF_RANGE:      http.StatusBadRequest,
	httperror.TSDB_CODE_PAR_INVALID_FILL_TIME_RANGE: http.StatusBadRequest,
	//401
	httperror.TSDB_CODE_MND_USER_ALREADY_EXIST:  http.StatusUnauthorized,
	httperror.TSDB_CODE_MND_USER_NOT_EXIST:      http.StatusUnauthorized,
	httperror.TSDB_CODE_MND_INVALID_USER_FORMAT: http.StatusUnauthorized,
	httperror.TSDB_CODE_MND_INVALID_PASS_FORMAT: http.StatusUnauthorized,
	httperror.TSDB_CODE_MND_NO_USER_FROM_CONN:   http.StatusUnauthorized,
	httperror.TSDB_CODE_MND_TOO_MANY_USERS:      http.StatusUnauthorized,
	httperror.TSDB_CODE_MND_INVALID_ALTER_OPER:  http.StatusUnauthorized,
	httperror.TSDB_CODE_MND_AUTH_FAILURE:        http.StatusUnauthorized,
	//502
	httperror.RPC_NETWORK_UNAVAIL: http.StatusBadGateway,
}

func TaosErrorResponse(c *gin.Context, code int, msg string) {
	code = code & 0xffff
	httpCode := getErrorHttpStatus(int32(code))
	errorResp(c, httpCode, code, msg)
}

func CommonErrorResponse(c *gin.Context, msg string) {
	httpCode := getErrorHttpStatus(0xffff)
	errorResp(c, httpCode, 0xffff, msg)
}

type MessageWithTiming struct {
	Code   int    `json:"code"`
	Desc   string `json:"desc"`
	Timing int64  `json:"timing"`
}

func errorResp(c *gin.Context, httpCode int, code int, msg string) {
	st, ok := c.Get(StartTimeKey)
	if ok {
		c.AbortWithStatusJSON(httpCode, &MessageWithTiming{
			Code:   code,
			Desc:   msg,
			Timing: time.Now().UnixNano() - st.(int64),
		})
	} else {
		c.AbortWithStatusJSON(httpCode, &Message{
			Code: code,
			Desc: msg,
		})
	}
	web.SetTaosErrorCode(c, code)
}

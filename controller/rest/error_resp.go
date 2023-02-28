package rest

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/httperror"
	"github.com/taosdata/taosadapter/v3/tools/web"
)

func UnAuthResponse(c *gin.Context, code int) {
	errStr, exist := httperror.ErrorMsgMap[code]
	if !exist {
		errStr = "unknown error"
	}
	UnAuthResponseWithMsg(c, code, errStr)
}

func UnAuthResponseWithMsg(c *gin.Context, code int, msg string) {
	c.AbortWithStatusJSON(http.StatusUnauthorized, &Message{
		Code: code,
		Desc: msg,
	})
	web.SetTaosErrorCode(c, code)
}

func BadRequestResponse(c *gin.Context, code int) {
	errStr := httperror.ErrorMsgMap[code]
	if len(errStr) == 0 {
		errStr = "unknown error"
	}
	BadRequestResponseWithMsg(c, code, errStr)
}

func BadRequestResponseWithMsg(c *gin.Context, code int, msg string) {
	c.AbortWithStatusJSON(http.StatusBadRequest, &Message{
		Code: code,
		Desc: msg,
	})
	web.SetTaosErrorCode(c, code)
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
	c.AbortWithStatusJSON(httpCode, &Message{
		Code: code,
		Desc: msg,
	})
	web.SetTaosErrorCode(c, code)
}

func CommonErrorResponse(c *gin.Context, msg string) {
	httpCode := getErrorHttpStatus(0xffff)
	c.AbortWithStatusJSON(httpCode, &Message{
		Code: 0xffff,
		Desc: msg,
	})
	web.SetTaosErrorCode(c, 0xffff)
}

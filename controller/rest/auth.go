package rest

import (
	"crypto/des"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/patrickmn/go-cache"
	"github.com/taosdata/taosadapter/httperror"
	"github.com/taosdata/taosadapter/tools"
	"github.com/taosdata/taosadapter/tools/pool"
	"github.com/taosdata/taosadapter/tools/web"
)

var authCache = cache.New(30*time.Minute, time.Hour)
var tokenCache = cache.New(30*time.Minute, time.Hour)

type authInfo struct {
	User     string
	Password string
}

var desKey = []byte{
	64,
	182,
	122,
	48,
	86,
	115,
	253,
	68,
}

func DecodeDes(auth string) (user, password string, err error) {
	d, err := base64.StdEncoding.DecodeString(auth)
	if err != nil {
		return "", "", err
	}
	if len(d) != 48 {
		return "", "", errors.New("wrong des length")
	}
	block, _ := des.NewCipher(desKey)
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	for i := 0; i < 6; i++ {
		origData := make([]byte, 8)
		block.Decrypt(origData, d[i*8:+(i+1)*8])
		b.Write(origData)
		if i == 2 {
			user, err = b.ReadString(0)
			if err == nil {
				user = user[:len(user)-1]
			}
			b.Reset()
		}
	}
	password, err = b.ReadString(0)
	if err == nil {
		password = password[:len(password)-1]
	}
	return user, password, nil
}

func EncodeDes(user, password string) (string, error) {
	if len(user) > 24 || len(password) > 24 {
		return "", errors.New("wrong user or password length")
	}

	b := make([]byte, 48)
	for i := 0; i < len(user); i++ {
		b[i] = user[i]
	}
	for i := 0; i < len(password); i++ {
		b[i+24] = password[i]
	}
	v, exist := tokenCache.Get(string(b))
	if exist {
		return v.(string), nil
	}
	block, _ := des.NewCipher(desKey)
	buf := pool.BytesPoolGet()
	defer pool.BytesPoolPut(buf)
	for i := 0; i < 6; i++ {
		d := make([]byte, 8)
		block.Encrypt(d, b[i*8:(i+1)*8])
		buf.Write(d)
	}
	data := base64.StdEncoding.EncodeToString(buf.Bytes())
	tokenCache.SetDefault(string(b), data)
	return data, nil
}

const (
	UserKey     = "user"
	PasswordKey = "password"
)

func CheckAuth(c *gin.Context) {
	auth := c.GetHeader("Authorization")
	if len(auth) == 0 {
		ErrorResponse(c, httperror.HTTP_NO_AUTH_INFO)
		return
	}
	auth = strings.TrimSpace(auth)
	v, exist := authCache.Get(auth)
	if exist {
		info := v.(*authInfo)
		c.Set(UserKey, info.User)
		c.Set(PasswordKey, info.Password)
		return
	}
	if strings.HasPrefix(auth, "Basic") {
		user, password, err := tools.DecodeBasic(auth[6:])
		if err != nil {
			ErrorResponse(c, httperror.HTTP_INVALID_BASIC_AUTH)
			return
		}
		if len(user) == 0 || len(password) == 0 {
			ErrorResponse(c, httperror.HTTP_INVALID_BASIC_AUTH)
			return
		}
		authCache.SetDefault(auth, &authInfo{
			User:     user,
			Password: password,
		})
		c.Set(UserKey, user)
		c.Set(PasswordKey, password)
	} else if strings.HasPrefix(auth, "Taosd") {
		user, password, err := DecodeDes(auth[6:])
		if err != nil {
			ErrorResponse(c, httperror.HTTP_INVALID_BASIC_AUTH)
			return
		}
		if len(user) == 0 || len(password) == 0 {
			ErrorResponse(c, httperror.HTTP_INVALID_BASIC_AUTH)
			return
		}
		authCache.SetDefault(auth, &authInfo{
			User:     user,
			Password: password,
		})
		c.Set(UserKey, user)
		c.Set(PasswordKey, password)
	} else {
		ErrorResponse(c, httperror.HTTP_INVALID_AUTH_TYPE)
		return
	}
}

type Message struct {
	Status string `json:"status"`
	Code   int    `json:"code"`
	Desc   string `json:"desc"`
}

func ErrorResponse(c *gin.Context, code int) {
	errStr := httperror.ErrorMsgMap[code]
	if len(errStr) == 0 {
		errStr = "unknown error"
	}
	c.AbortWithStatusJSON(http.StatusOK, &Message{
		Status: "error",
		Code:   code,
		Desc:   errStr,
	})
	web.SetTaosErrorCode(c, code)
}

func ErrorResponseWithMsg(c *gin.Context, code int, msg string) {
	c.AbortWithStatusJSON(http.StatusOK, &Message{
		Status: "error",
		Code:   code & 0xffff,
		Desc:   msg,
	})
	web.SetTaosErrorCode(c, code&0xffff)
}

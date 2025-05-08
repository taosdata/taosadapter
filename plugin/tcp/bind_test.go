package tcp

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db"
	"github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/log"
)

func TestBind(t *testing.T) {
	config.Init()
	log.ConfigLog()
	db.PrepareConnection()
	viper.Set("tcp.enable", true)
	log.SetLevel("debug")
	s := NewServer()
	err := s.Init(nil)
	assert.NoError(t, err)
	err = s.Start()
	assert.NoError(t, err)
	defer s.Stop()
	tcpConn, err := net.Dial("tcp", "127.0.0.1:6042")
	assert.NoError(t, err)
	defer tcpConn.Close()
	// connect to the server

	// length uint32
	// sequence uint64
	// version uint8
	// command uint8
	// reserved [6]byte
	// signal [8]byte
	// payload []byte
	buf := marshalConnReq(1, "root", "taosdata")
	n, err := tcpConn.Write(buf)
	assert.NoError(t, err)
	assert.Equal(t, len(buf), n)
	reader := bufio.NewReader(tcpConn)
	bs, err := readPacket(reader)
	if err != nil {
		panic(err)
	}
	fmt.Println(bs)
}
func marshalConnReq(reqID int, user string, pass string) []byte {
	// length uint32
	// sequence uint64
	// version uint8
	// command uint8
	// reserved [6]byte
	// signal [8]byte

	// version 1
	// userLen uint8
	// user string
	// passwordLen uint8
	// password string
	// dbLen uint8
	// db string
	// set_mode bool
	// mode int32
	// tzLen uint16
	// tz string
	// appLen uint16
	// app string
	// ipLen uint16
	// ip string
	bufLen := 28 + 1 + 1 + len(user) + 1 + len(pass) + 1 + 1 + 4 + 2 + 2 + 2
	buf := make([]byte, bufLen)
	binary.LittleEndian.PutUint32(buf, uint32(bufLen-4))
	binary.LittleEndian.PutUint64(buf[4:], uint64(reqID))
	buf[12] = 1
	buf[13] = CmdConn
	buf[28] = 1
	buf[29] = uint8(len(user))
	copy(buf[30:], user)
	offset := 30 + len(user)
	buf[offset] = uint8(len(pass))
	offset++
	copy(buf[offset:], pass)
	return buf
}

func readPacket(conn *bufio.Reader) ([]byte, error) {
	//[1 0 0 0
	//0 0 0 0 1 2 0 0 0 0 0 0 0 0 0 0 0 0 0 0 170 62 226 49 2 0 0 0 0 0 0 0 0 0]
	// length uint32 4
	// sequence uint64 8
	// version uint8  1
	// command uint8  1
	// reserved [6]byte 6
	// signal [8]byte 8
	// timing uint64 8
	// code int32 4
	// messageLen uint8
	// message []byte
	// payload []byte
	lengthBuf := make([]byte, 4)
	_, err := conn.Read(lengthBuf)
	if err != nil {
		return nil, err
	}
	length := binary.LittleEndian.Uint32(lengthBuf)
	buf := make([]byte, length)
	_, err = io.ReadFull(conn, buf)
	if err != nil {
		return nil, err
	}
	code := binary.LittleEndian.Uint32(buf[32:])
	if code != 0 {
		messageLen := int(buf[36])
		return nil, &errors.TaosError{Code: int32(int(code)), ErrStr: string(buf[37 : 37+messageLen])}
	}
	return buf, nil
}

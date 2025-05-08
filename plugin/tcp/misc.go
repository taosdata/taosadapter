package tcp

import (
	"context"
	"encoding/binary"
	"math"

	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/controller/ws/wstool"
	"github.com/taosdata/taosadapter/v3/version"
)

type versionResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Action  string `json:"action"`
	ReqID   uint64 `json:"req_id"`
	Timing  int64  `json:"timing"`
	Version string `json:"version"`
}

const ResponseHeaderLen = 4 + 8 + 1 + 1 + 6 + 8 + 8 + 4 + 1
const ReqIDOffset = 4
const VersionOffset = ReqIDOffset + 8
const CommandOffset = VersionOffset + 1
const SignalOffset = CommandOffset + 7
const TimingOffset = SignalOffset + 8
const CodeOffset = TimingOffset + 8
const MessageLenOffset = CodeOffset + 4

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
func marshalCommonResponse(buf []byte, ctx context.Context, reqID uint64, code uint32, message string, cmd byte) {
	binary.LittleEndian.PutUint64(buf[ReqIDOffset:], reqID)
	buf[VersionOffset] = 1
	buf[CommandOffset] = cmd
	binary.LittleEndian.PutUint64(buf[TimingOffset:], uint64(wstool.GetDuration(ctx)))
	if code != 0 {
		binary.LittleEndian.PutUint32(buf[CodeOffset:], code)
		buf[MessageLenOffset] = uint8(len(message))
		copy(buf[MessageLenOffset+1:], message)
	}
}

func (c *Connection) sendErrorResponse(ctx context.Context, reqID uint64, code uint32, message string, cmd byte) {
	if len(message) > math.MaxUint8 {
		message = message[:math.MaxUint8]
	}
	buf := make([]byte, ResponseHeaderLen+len(message)+1)
	marshalCommonResponse(buf, ctx, reqID, code, message, cmd)
	c.writePacket(buf)
}

func (c *Connection) sendCommonSuccessResponse(ctx context.Context, reqID uint64, cmd byte) {
	buf := make([]byte, ResponseHeaderLen)
	marshalCommonResponse(buf, ctx, reqID, 0, "", cmd)
	c.writePacket(buf)
}

func (c *Connection) sendStmtCommonResponse(ctx context.Context, reqID uint64, stmtID uint64, code uint32, message string, cmd byte) {
	if len(message) > math.MaxUint8 {
		message = message[:math.MaxUint8]
	}
	totalLen := ResponseHeaderLen + len(message) + 1 + 8 + 1
	buf := make([]byte, totalLen)
	marshalCommonResponse(buf, ctx, reqID, code, message, cmd)
	buf[totalLen-9] = uint8(1)
	binary.LittleEndian.PutUint64(buf[totalLen-8:], stmtID)
	c.writePacket(buf)
}
func marshalString(buf []byte, str string) {
	strLen := len(str)
	if strLen > math.MaxUint16 {
		strLen = math.MaxUint16
	}
	binary.LittleEndian.PutUint16(buf, uint16(strLen))
	copy(buf[2:], str)
}

func MarshalVersionResponse(ctx context.Context, reqID uint64, version string) []byte {
	versionLen := len(version)
	if versionLen > math.MaxUint16 {
		versionLen = math.MaxUint16
	}
	bufLen := ResponseHeaderLen + versionLen + 2
	buf := make([]byte, bufLen)
	marshalCommonResponse(buf, ctx, reqID, 0, "", CmdVersion)
	marshalString(buf[MessageLenOffset+1:], version)
	return buf
}

func (c *Connection) handleVersion(ctx context.Context, reqID uint64, logger *logrus.Entry) {
	logger.Trace("get version")
	buf := MarshalVersionResponse(ctx, reqID, version.TaosClientVersion)
	c.writePacket(buf)
}

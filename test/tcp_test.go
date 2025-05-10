package test

import (
	"bufio"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/taosadapter/v3/driver/common/stmt"
	"github.com/taosdata/taosadapter/v3/driver/errors"
)

const (
	tableCount = 10
	rows       = 10000
	loopTimes  = 5000
)

const (
	CmdVersion byte = iota + 1
	CmdConn
	CmdStmt2Init
	CmdStmt2Prepare
	CmdStmt2Bind
	CmdStmt2Execute
	CmdStmt2Close
	CmdTotal
)

var reqIncrement int64

func GetReqID() int64 {
	id := atomic.AddInt64(&reqIncrement, 1)
	if id > 0x00ffffffffffffff {
		atomic.StoreInt64(&reqIncrement, 1)
		id = 1
	}
	reqId := int64(32)<<56 | id
	return reqId
}
func TestTcpSingle(t *testing.T) {

	// connect to the server
	tcpConn, err := net.Dial("tcp", "172.16.1.43:6042")
	if err != nil {
		panic(err)
	}
	defer tcpConn.Close()
	// length uint32
	// sequence uint64
	// version uint8
	// command uint8
	// reserved [6]byte
	// signal [8]byte
	// payload []byte
	reqID := uint64(GetReqID())
	buf := marshalConnReq(reqID, "root", "taosdata", "test")
	n, err := tcpConn.Write(buf)
	assert.NoError(t, err)
	assert.Equal(t, len(buf), n)
	reader := bufio.NewReader(tcpConn)
	bs, err := readPacket(reader)
	if err != nil {
		panic(err)
	}
	respReqID, timing := unmarshalConnResp(bs)
	assert.Equal(t, reqID, respReqID)
	assert.Greater(t, timing, int64(0))

	reqID = uint64(GetReqID())
	buf = marshalStmt2InitReq(reqID)
	n, err = tcpConn.Write(buf)
	assert.NoError(t, err)
	assert.Equal(t, len(buf), n)
	bs, err = readPacket(reader)
	if err != nil {
		panic(err)
	}
	//t.Log(len(bs))
	respReqID, timing, version, stmtID := unmarshalStmt2InitResp(bs)
	assert.Equal(t, reqID, respReqID)
	assert.Greater(t, timing, int64(0))
	assert.Equal(t, uint8(1), version)
	assert.Equal(t, uint64(1), stmtID)

	//prepare
	sql := "insert into ? using meters tags(?,?) values(?,?,?,?)"
	reqID = uint64(GetReqID())
	buf = marshalStmt2Prepare(reqID, stmtID, sql)
	n, err = tcpConn.Write(buf)
	assert.NoError(t, err)
	assert.Equal(t, len(buf), n)
	bs, err = readPacket(reader)
	if err != nil {
		panic(err)
	}
	//t.Log(bs)
	respReqID, timing, version, stmtID, isInsert, fieldsCount, fields := unmarshalStmt2PrepareResp(bs)
	assert.Equal(t, reqID, respReqID)
	assert.Greater(t, timing, int64(0))
	assert.Equal(t, uint8(1), version)
	assert.Equal(t, uint64(1), stmtID)
	assert.Equal(t, true, isInsert)
	assert.Equal(t, int32(7), fieldsCount)
	//t.Log(fields)
	// bind
	binds := make([]*stmt.TaosStmt2BindData, tableCount)
	ts := time.Now().UnixMilli()
	for i := 0; i < tableCount; i++ {
		tbName := fmt.Sprintf("t_%04d", i)
		tag := []driver.Value{int32(i), "location"}
		cols := make([][]driver.Value, 4)
		for j := 0; j < 4; j++ {
			cols[j] = make([]driver.Value, rows)
		}
		for j := 0; j < rows; j++ {
			// ts                             | TIMESTAMP              |           8 |                    | delta-i        | lz4            | medium         |
			// current                        | FLOAT                  |           4 |                    | delta-d        | lz4            | medium         |
			// voltage                        | INT                    |           4 |                    | simple8b       | lz4            | medium         |
			// phase                          | FLOAT                  |           4 |                    | delta-d        | lz4            | medium         |
			cols[0][j] = ts + int64(j)
			cols[1][j] = float32(j)
			cols[2][j] = int32(j)
			cols[3][j] = float32(j)
		}
		bind := &stmt.TaosStmt2BindData{
			TableName: tbName,
			Tags:      tag,
			Cols:      cols,
		}
		binds[i] = bind
	}
	body, err := stmt.MarshalStmt2Binary(binds, true, fields)
	assert.NoError(t, err)
	bindBuf := marshalStmt2BindReq(reqID, stmtID, body)
	start := time.Now()
	for i := 0; i < loopTimes; i++ {
		n, err = tcpConn.Write(bindBuf)
		assert.NoError(t, err)
		assert.Equal(t, len(bindBuf), n)
		bs, err = readPacket(reader)
		if err != nil {
			panic(err)
		}
		//t.Log(len(bs))
		respReqID, timing, version, stmtID = unmarshalStmt2BindResp(bs)
		assert.Equal(t, reqID, respReqID)
		assert.Greater(t, timing, int64(0))
		assert.Equal(t, uint8(1), version)
		assert.Equal(t, uint64(1), stmtID)
		// exec
		buf = marshalStmt2ExecReq(reqID, stmtID)
		n, err = tcpConn.Write(buf)
		assert.NoError(t, err)
		assert.Equal(t, len(buf), n)
		bs, err = readPacket(reader)
		if err != nil {
			panic(err)
		}
		//t.Log(len(bs))
		respReqID, timing, version, stmtID, affected := unmarshalStmt2ExecResp(bs)
		assert.Equal(t, reqID, respReqID)
		assert.Greater(t, timing, int64(0))
		assert.Equal(t, uint8(1), version)
		assert.Equal(t, uint64(1), stmtID)
		_ = affected
		//assert.Equal(t, int32(1000), affected)
	}
	t.Log(time.Now().Sub(start))
	// close
	buf = marshalStmt2CloseReq(reqID, stmtID)
	n, err = tcpConn.Write(buf)
	assert.NoError(t, err)
	assert.Equal(t, len(buf), n)
	bs, err = readPacket(reader)
	if err != nil {
		panic(err)
	}
	//t.Log(len(bs))
	respReqID, timing, version, stmtID = unmarshalStmt2CloseResp(bs)
	assert.Equal(t, reqID, respReqID)
	assert.Greater(t, timing, int64(0))
	assert.Equal(t, uint8(1), version)
	assert.Equal(t, uint64(1), stmtID)
}

func TestTcpConcurrent(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(8)
	for i := 0; i < 8; i++ {
		go func() {
			defer wg.Done()
			TestTcpSingle(t)
		}()
	}
	wg.Wait()
}

func marshalConnReq(reqID uint64, user string, pass string, db string) []byte {
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
	binary.LittleEndian.PutUint64(buf[4:], reqID)
	buf[12] = 1
	buf[13] = CmdConn
	buf[28] = 1
	buf[29] = uint8(len(user))
	copy(buf[30:], user)
	offset := 30 + len(user)
	buf[offset] = uint8(len(pass))
	offset++
	copy(buf[offset:], pass)
	offset += len(pass)
	buf[offset] = uint8(len(db))
	offset++
	copy(buf[offset:], db)
	return buf
}

const headerLen = 28

func marshalStmt2InitReq(reqID uint64) []byte {
	// length uint32
	// sequence uint64
	// version uint8
	// command uint8
	// reserved [6]byte
	// signal [8]byte

	// version 1
	// SingleStbInsert bool
	// SingleTableBindOnce bool
	bufLen := headerLen + 3
	buf := make([]byte, bufLen)
	binary.LittleEndian.PutUint32(buf, uint32(bufLen-4))
	binary.LittleEndian.PutUint64(buf[4:], reqID)
	buf[12] = 1
	buf[13] = CmdStmt2Init
	buf[28] = 1
	buf[29] = 1
	buf[30] = 1
	return buf
}

const payloadOffset = 37

func unmarshalStmt2InitResp(buf []byte) (reqID uint64, timing int64, version uint8, stmtID uint64) {
	reqID = binary.LittleEndian.Uint64(buf)
	timing = int64(binary.LittleEndian.Uint64(buf[24:]))
	version = buf[payloadOffset]
	stmtID = binary.LittleEndian.Uint64(buf[payloadOffset+1:])
	return
}

func marshalStmt2Prepare(reqID uint64, stmtID uint64, sql string) []byte {
	// length uint32
	// sequence uint64
	// version uint8
	// command uint8
	// reserved [6]byte
	// signal [8]byte

	// version 1
	// StmtID uint64
	// SQLLen uint32
	// SQL string
	// GetFields bool
	bufLen := headerLen + 1 + 8 + 4 + len(sql) + 1
	buf := make([]byte, bufLen)
	binary.LittleEndian.PutUint32(buf, uint32(bufLen-4))
	binary.LittleEndian.PutUint64(buf[4:], reqID)
	buf[12] = 1
	buf[13] = CmdStmt2Prepare
	buf[28] = 1
	binary.LittleEndian.PutUint64(buf[29:], stmtID)
	binary.LittleEndian.PutUint32(buf[37:], uint32(len(sql)))
	copy(buf[41:], sql)
	buf[41+len(sql)] = 1
	return buf
}

func unmarshalStmt2PrepareResp(buf []byte) (reqID uint64, timing int64, version uint8, stmtID uint64, isInsert bool, fieldsCount int32, fields []*stmt.Stmt2AllField) {
	reqID = binary.LittleEndian.Uint64(buf)
	timing = int64(binary.LittleEndian.Uint64(buf[24:]))
	payload := buf[payloadOffset:]
	_ = payload
	version = buf[payloadOffset]
	stmtID = binary.LittleEndian.Uint64(buf[payloadOffset+1:])
	isInsert = buf[payloadOffset+9] == 1
	fieldsCount = int32(binary.LittleEndian.Uint32(buf[payloadOffset+10:]))
	offset := payloadOffset + 14
	fields = make([]*stmt.Stmt2AllField, fieldsCount)
	//type Stmt2AllField struct {
	//	Name      string `json:"name"`
	//	FieldType int8   `json:"field_type"`
	//	Precision uint8  `json:"precision"`
	//	Scale     uint8  `json:"scale"`
	//	Bytes     int32  `json:"bytes"`
	//	BindType  int8   `json:"bind_type"`
	//}
	for i := 0; i < len(fields); i++ {
		fields[i] = &stmt.Stmt2AllField{}
		fieldNameLen := int(buf[offset])
		offset++
		fields[i].Name = string(buf[offset : offset+fieldNameLen])
		offset += fieldNameLen
		fields[i].FieldType = int8(buf[offset])
		offset++
		fields[i].Precision = buf[offset]
		offset++
		fields[i].Scale = buf[offset]
		offset++
		fields[i].Bytes = int32(binary.LittleEndian.Uint32(buf[offset:]))
		offset += 4
		fields[i].BindType = int8(buf[offset])
		offset++
	}
	return
}

type connResp struct {
	ReqID  uint64 `json:"req_id"`
	Timing int64  `json:"timing"`
}

func unmarshalConnResp(buf []byte) (reqID uint64, timing int64) {
	reqID = binary.LittleEndian.Uint64(buf)
	timing = int64(binary.LittleEndian.Uint64(buf[24:]))
	return
}

func marshalStmt2BindReq(reqID uint64, stmt2ID uint64, body []byte) []byte {
	// length uint32
	// sequence uint64
	// version uint8
	// command uint8
	// reserved [6]byte
	// signal [8]byte

	// version 1
	// StmtID uint64
	// colIndex int32
	// MessageLen uint64
	// Message []byte
	bufLen := headerLen + 1 + 8 + 4 + 8 + len(body)
	buf := make([]byte, bufLen)
	binary.LittleEndian.PutUint32(buf, uint32(bufLen-4))
	binary.LittleEndian.PutUint64(buf[4:], reqID)
	buf[12] = 1
	buf[13] = CmdStmt2Bind
	buf[headerLen] = 1
	colIndex := int32(-1)
	binary.LittleEndian.PutUint64(buf[headerLen+1:headerLen+9], stmt2ID)
	binary.LittleEndian.PutUint32(buf[headerLen+9:headerLen+13], uint32(colIndex))
	binary.LittleEndian.PutUint64(buf[headerLen+13:headerLen+21], uint64(len(body)))
	copy(buf[headerLen+21:], body)
	return buf
}

func unmarshalStmt2BindResp(buf []byte) (reqID uint64, timing int64, version uint8, stmtID uint64) {
	return unmarshalStmt2InitResp(buf)
}

func marshalStmt2ExecReq(reqID uint64, stmt2ID uint64) []byte {
	// length uint32
	// sequence uint64
	// version uint8
	// command uint8
	// reserved [6]byte
	// signal [8]byte

	// version 1
	// StmtID uint64
	bufLen := headerLen + 1 + 8
	buf := make([]byte, bufLen)
	binary.LittleEndian.PutUint32(buf, uint32(bufLen-4))
	binary.LittleEndian.PutUint64(buf[4:], reqID)
	buf[12] = 1
	buf[13] = CmdStmt2Execute
	buf[headerLen] = 1
	binary.LittleEndian.PutUint64(buf[headerLen+1:], stmt2ID)
	return buf
}

func unmarshalStmt2ExecResp(buf []byte) (reqID uint64, timing int64, version uint8, stmtID uint64, affectedRows int32) {
	reqID = binary.LittleEndian.Uint64(buf)
	timing = int64(binary.LittleEndian.Uint64(buf[24:]))
	version = buf[payloadOffset]
	stmtID = binary.LittleEndian.Uint64(buf[payloadOffset+1:])
	affectedRows = int32(binary.LittleEndian.Uint32(buf[payloadOffset+9:]))
	return
}

func marshalStmt2CloseReq(reqID uint64, stmt2ID uint64) []byte {
	// length uint32
	// sequence uint64
	// version uint8
	// command uint8
	// reserved [6]byte
	// signal [8]byte

	// version 1
	// StmtID uint64
	bufLen := headerLen + 1 + 8
	buf := make([]byte, bufLen)
	binary.LittleEndian.PutUint32(buf, uint32(bufLen-4))
	binary.LittleEndian.PutUint64(buf[4:], reqID)
	buf[12] = 1
	buf[13] = CmdStmt2Close
	buf[headerLen] = 1
	binary.LittleEndian.PutUint64(buf[headerLen+1:], stmt2ID)
	return buf
}

func unmarshalStmt2CloseResp(buf []byte) (reqID uint64, timing int64, version uint8, stmtID uint64) {
	reqID = binary.LittleEndian.Uint64(buf)
	timing = int64(binary.LittleEndian.Uint64(buf[24:]))
	version = buf[payloadOffset]
	stmtID = binary.LittleEndian.Uint64(buf[payloadOffset+1:])
	return
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

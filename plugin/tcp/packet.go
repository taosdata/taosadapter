package tcp

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"time"
)

type packet struct {
	conn         net.Conn
	reader       *bufio.Reader
	bufWriter    *bufio.Writer
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func newPacket(conn net.Conn, readTimeout time.Duration) *packet {
	return &packet{
		conn:        conn,
		reader:      bufio.NewReader(conn),
		bufWriter:   bufio.NewWriter(conn),
		readTimeout: readTimeout,
	}
}

func (p *packet) readOnePacket() ([]byte, error) {
	// length uint32

	var header [4]byte
	if p.readTimeout > 0 {
		if err := p.conn.SetReadDeadline(time.Now().Add(p.readTimeout)); err != nil {
			return nil, err
		}
	}
	if _, err := io.ReadFull(p.reader, header[:]); err != nil {
		return nil, err
	}
	length := binary.LittleEndian.Uint32(header[:])
	data := make([]byte, length)
	if p.readTimeout > 0 {
		if err := p.conn.SetReadDeadline(time.Now().Add(p.readTimeout)); err != nil {
			return nil, err
		}
	}
	if _, err := io.ReadFull(p.reader, data); err != nil {
		return nil, err
	}
	return data, nil
}

func (p *packet) readPacket() ([]byte, error) {
	if p.readTimeout == 0 {
		if err := p.conn.SetReadDeadline(time.Time{}); err != nil {
			return nil, err
		}
	}
	data, err := p.readOnePacket()
	if err != nil {
		return nil, err
	}
	return data, nil
}

var ErrBadConn = errors.New("bad connection")

func (p *packet) writePacket(data []byte) error {
	length := len(data) - 4
	binary.LittleEndian.PutUint32(data, uint32(length))
	if n, err := p.bufWriter.Write(data); err != nil {
		return ErrBadConn
	} else if n != len(data) {
		return ErrBadConn
	}
	p.bufWriter.Flush()
	return nil
}

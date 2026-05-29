package stmt

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/taosdata/taosadapter/v3/driver/common"
)

const (
	Stmt2ColumnTotalLengthPosition = 0
	Stmt2ColumnRowCountPosition    = Stmt2ColumnTotalLengthPosition + 4
	Stmt2ColumnTableCountPosition  = Stmt2ColumnRowCountPosition + 4
	Stmt2ColumnFieldCountPosition  = Stmt2ColumnTableCountPosition + 4
	Stmt2ColumnFieldOffsetPosition = Stmt2ColumnFieldCountPosition + 4
	Stmt2ColumnDataPosition        = Stmt2ColumnFieldOffsetPosition + 4
)

func MarshalStmt2ColumnBinary(columns [][]driver.Value, fields []*Stmt2AllField) ([]byte, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("empty columns")
	}
	rowCount := len(columns[0])
	if rowCount == 0 {
		return nil, fmt.Errorf("empty rows")
	}
	for i := range columns {
		if len(columns[i]) != rowCount {
			return nil, fmt.Errorf("row count not match, column:%d, got:%d, expect:%d", i, len(columns[i]), rowCount)
		}
	}
	if len(fields) == 0 {
		return marshalStmt2ColumnBinaryQuery(columns, rowCount)
	}
	if len(columns) != len(fields) {
		return nil, fmt.Errorf("column count not match, data count:%d, field count:%d", len(columns), len(fields))
	}
	hasQueryField := false
	tbNameIndex := -1
	for i := range fields {
		switch fields[i].BindType {
		case TAOS_FIELD_QUERY:
			hasQueryField = true
		case TAOS_FIELD_TBNAME:
			if tbNameIndex == -1 {
				tbNameIndex = i
			}
		}
	}
	if hasQueryField && rowCount != 1 {
		return nil, fmt.Errorf("query only supports one row")
	}

	tableCount := uint32(1)
	if tbNameIndex >= 0 {
		count, err := stmt2ColumnTableCount(columns[tbNameIndex])
		if err != nil {
			return nil, err
		}
		tableCount = count
	}

	buffer := make([]byte, Stmt2ColumnDataPosition)
	binary.LittleEndian.PutUint32(buffer[Stmt2ColumnRowCountPosition:], uint32(rowCount))
	binary.LittleEndian.PutUint32(buffer[Stmt2ColumnTableCountPosition:], tableCount)
	binary.LittleEndian.PutUint32(buffer[Stmt2ColumnFieldCountPosition:], uint32(len(fields)))
	binary.LittleEndian.PutUint32(buffer[Stmt2ColumnFieldOffsetPosition:], uint32(Stmt2ColumnDataPosition))

	for i := range fields {
		block, err := stmt2ColumnBlock(columns[i], fields[i])
		if err != nil {
			return nil, fmt.Errorf("generate column block %d error: %w", i, err)
		}
		buffer = append(buffer, block...)
	}
	binary.LittleEndian.PutUint32(buffer[Stmt2ColumnTotalLengthPosition:], uint32(len(buffer)))
	return buffer, nil
}

func marshalStmt2ColumnBinaryQuery(columns [][]driver.Value, rowCount int) ([]byte, error) {
	if rowCount != 1 {
		return nil, fmt.Errorf("query only supports one row")
	}
	buffer := make([]byte, Stmt2ColumnDataPosition)
	binary.LittleEndian.PutUint32(buffer[Stmt2ColumnRowCountPosition:], uint32(rowCount))
	binary.LittleEndian.PutUint32(buffer[Stmt2ColumnTableCountPosition:], 1)
	binary.LittleEndian.PutUint32(buffer[Stmt2ColumnFieldCountPosition:], uint32(len(columns)))
	binary.LittleEndian.PutUint32(buffer[Stmt2ColumnFieldOffsetPosition:], uint32(Stmt2ColumnDataPosition))
	for i := range columns {
		block, err := generateBindQueryColumnData(columns[i][0], nil)
		if err != nil {
			return nil, fmt.Errorf("generate query column block %d error: %w", i, err)
		}
		buffer = append(buffer, block...)
	}
	binary.LittleEndian.PutUint32(buffer[Stmt2ColumnTotalLengthPosition:], uint32(len(buffer)))
	return buffer, nil
}

func stmt2ColumnBlock(column []driver.Value, field *Stmt2AllField) ([]byte, error) {
	if field.BindType == TAOS_FIELD_QUERY {
		if len(column) != 1 {
			return nil, fmt.Errorf("query only supports one row")
		}
		if column[0] != nil {
			return generateBindQueryColumnData(column[0], field)
		}
	}
	return generateBindColData(column, field, &bytes.Buffer{})
}

func generateBindQueryColumnData(data driver.Value, field *Stmt2AllField) ([]byte, error) {
	switch v := data.(type) {
	case time.Time:
		precision := common.PrecisionMilliSecond
		if field != nil && field.FieldType == common.TSDB_DATA_TYPE_TIMESTAMP {
			precision = int(field.Precision)
		}
		var ts int64
		switch precision {
		case common.PrecisionNanoSecond:
			ts = v.UnixNano()
		case common.PrecisionMicroSecond:
			ts = v.UnixNano() / 1e3
		default:
			ts = v.UnixNano() / 1e6
		}
		return stmt2SingleRowBindData(common.TSDB_DATA_TYPE_TIMESTAMP, false, 0, func(buf []byte) {
			binary.LittleEndian.PutUint64(buf, uint64(ts))
		}), nil
	default:
		return generateBindQueryData(data)
	}
}

func stmt2SingleRowBindData(fieldType int8, hasLength bool, valueLength int32, fillBuffer func(buf []byte)) []byte {
	headerLength := getBindDataHeaderLength(1, hasLength)
	bufferLength := 0
	if fillBuffer != nil {
		bufferLength = int(valueLength)
		if !hasLength {
			bufferLength = typeSize(fieldType)
		}
	}
	totalLength := headerLength + bufferLength
	dataBuf := make([]byte, totalLength)
	binary.LittleEndian.PutUint32(dataBuf[BindDataTypeOffset:], uint32(fieldType))
	binary.LittleEndian.PutUint32(dataBuf[BindDataNumOffset:], 1)
	if hasLength {
		dataBuf[BindDataIsNullOffset+1] = 1
		binary.LittleEndian.PutUint32(dataBuf[BindDataIsNullOffset+2:], uint32(valueLength))
	}
	binary.LittleEndian.PutUint32(dataBuf[headerLength-4:], uint32(bufferLength))
	if fillBuffer != nil {
		fillBuffer(dataBuf[headerLength:])
	}
	binary.LittleEndian.PutUint32(dataBuf[BindDataTotalLengthOffset:], uint32(totalLength))
	return dataBuf
}

func typeSize(fieldType int8) int {
	switch fieldType {
	case common.TSDB_DATA_TYPE_BOOL,
		common.TSDB_DATA_TYPE_TINYINT,
		common.TSDB_DATA_TYPE_UTINYINT:
		return 1
	case common.TSDB_DATA_TYPE_SMALLINT,
		common.TSDB_DATA_TYPE_USMALLINT:
		return 2
	case common.TSDB_DATA_TYPE_INT,
		common.TSDB_DATA_TYPE_UINT,
		common.TSDB_DATA_TYPE_FLOAT:
		return 4
	case common.TSDB_DATA_TYPE_BIGINT,
		common.TSDB_DATA_TYPE_UBIGINT,
		common.TSDB_DATA_TYPE_DOUBLE,
		common.TSDB_DATA_TYPE_TIMESTAMP:
		return 8
	default:
		return 0
	}
}

func stmt2ColumnTableCount(tbNames []driver.Value) (uint32, error) {
	if len(tbNames) == 0 {
		return 0, fmt.Errorf("empty tbname column")
	}
	var count uint32
	var last string
	for i := range tbNames {
		name, err := stmt2ColumnTableName(tbNames[i])
		if err != nil {
			return 0, fmt.Errorf("invalid tbname at row %d: %w", i, err)
		}
		if i == 0 || name != last {
			count++
			last = name
		}
	}
	return count, nil
}

func stmt2ColumnTableName(value driver.Value) (string, error) {
	switch v := value.(type) {
	case string:
		if v == "" {
			return "", fmt.Errorf("empty table name")
		}
		return v, nil
	case []byte:
		if len(v) == 0 {
			return "", fmt.Errorf("empty table name")
		}
		return string(v), nil
	default:
		return "", fmt.Errorf("expect string or []byte, got %T", value)
	}
}

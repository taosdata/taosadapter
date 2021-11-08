package schemaless

import (
	"database/sql/driver"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/taosdata/driver-go/v2/af"
	tErrors "github.com/taosdata/driver-go/v2/errors"
	"github.com/taosdata/taosadapter/log"
	"github.com/taosdata/taosadapter/tools"
	"github.com/taosdata/taosadapter/tools/pool"
)

const (
	NCHARType   = "NCHAR"
	DOUBLEType  = "DOUBLE"
	BIGINTType  = "BIGINT"
	BINARYType  = "BINARY"
	UBIGINTType = "BIGINT UNSIGNED"
	BOOLType    = "BOOL"
)

var logger = log.GetLogger("schemaless")

type InsertLine struct {
	DB         string
	Ts         time.Time
	TableName  string
	STableName string
	Fields     map[string]interface{}
	TagNames   []string
	TagValues  []string
}

type Executor struct {
	conn *af.Connector
}

func NewExecutor(conn unsafe.Pointer) (*Executor, error) {
	afConnector, err := af.NewConnector(conn)
	if err != nil {
		return nil, err
	}
	return &Executor{conn: afConnector}, nil
}

func (e *Executor) InsertTDengine(line *InsertLine) (string, error) {
	//insert into table() using stable(tagName ...) tags(tagValue...) (field ...) values (values...)
	sql := e.generateInsertSql(line)
	if len(line.DB) == 0 {
		return sql, tErrors.ErrMndDbNotSelected
	}
	if len(line.STableName) == 0 {
		return sql, tErrors.ErrTscLineSyntaxError
	}
	if len(line.TableName) == 0 {
		return sql, tErrors.ErrTscLineSyntaxError
	}
	if len(line.Fields) == 0 {
		return sql, tErrors.ErrTscLineSyntaxError
	}
	if len(line.TagNames) == 0 {
		return sql, tErrors.ErrTscLineSyntaxError
	}
	if len(line.TagNames) != len(line.TagValues) {
		return sql, tErrors.ErrTscLineSyntaxError
	}
	haveNotNullValue := false
	for _, v := range line.Fields {
		if v != nil {
			haveNotNullValue = true
			break
		}
	}
	if !haveNotNullValue {
		return sql, tErrors.ErrTscLineSyntaxError
	}
	err := e.DoExec(sql)
	if err != nil {
		var tdErr *tErrors.TaosError
		if errors.As(err, &tdErr) {
			switch tdErr.Code {
			case tErrors.MND_INVALID_TABLE_NAME:
				//stable not exist
				err = e.createStable(line)
				if err != nil {
					return sql, err
				}
				tableInfo, err := e.DescribeTable(line.DB, line.STableName)
				if err != nil {
					return sql, err
				}
				err = e.modifyTag(line.DB, line.STableName, tableInfo, line.TagNames, line.TagValues)
				if err != nil {
					return sql, err
				}
				err = e.modifyColumn(line.DB, line.STableName, tableInfo, line.Fields)
				if err != nil {
					return sql, err
				}
			case tErrors.TSC_INVALID_OPERATION:
				tableInfo, err := e.DescribeTable(line.DB, line.STableName)
				if err != nil {
					return sql, err
				}
				err = e.modifyTag(line.DB, line.STableName, tableInfo, line.TagNames, line.TagValues)
				if err != nil {
					return sql, err
				}
				err = e.modifyColumn(line.DB, line.STableName, tableInfo, line.Fields)
				if err != nil {
					return sql, err
				}
			case tErrors.MND_DB_NOT_SELECTED:
				//db not exist
				err = e.createDatabase(line.DB)
				if err != nil {
					return sql, err
				}
				err = e.createStable(line)
				if err != nil {
					return sql, err
				}
				tableInfo, err := e.DescribeTable(line.DB, line.STableName)
				if err != nil {
					return sql, err
				}
				err = e.modifyTag(line.DB, line.STableName, tableInfo, line.TagNames, line.TagValues)
				if err != nil {
					return sql, err
				}
				err = e.modifyColumn(line.DB, line.STableName, tableInfo, line.Fields)
				if err != nil {
					return sql, err
				}
			case tErrors.TSC_SQL_SYNTAX_ERROR:
				if strings.Contains(tdErr.ErrStr, "string data overflow") {
					tableInfo, err := e.DescribeTable(line.DB, line.STableName)
					if err != nil {
						return sql, err
					}
					err = e.modifyColumn(line.DB, line.STableName, tableInfo, line.Fields)
					if err != nil {
						return sql, err
					}
				}
			default:
				return sql, err
			}
		} else {
			logger.WithError(err).WithField("sql", sql).Error("first insert sql error")
			return sql, err
		}
		err = e.DoExec(sql)
		if err != nil {
			logger.WithError(err).WithField("sql", sql).Error("reinsert sql error")
			return sql, err
		}
	}
	return sql, nil
}

func (e *Executor) createDatabase(db string) error {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("create database if not exists ")
	b.WriteString(db)
	b.WriteString(" precision 'ns' update 2")
	return e.DoExec(b.String())
}

func (e *Executor) createStable(info *InsertLine) error {
	tags := make([]*FieldInfo, 0, len(info.TagNames))
	for i, s := range info.TagNames {
		tags = append(tags, &FieldInfo{
			Name:   tools.RepairName(s),
			Type:   BINARYType,
			Length: len(info.TagValues[i]),
		})
	}
	columns := make([]*FieldInfo, 0, len(info.Fields))
	for columnName, columnValue := range info.Fields {
		filed := e.createFieldInfo(columnName, columnValue)
		if filed == nil {
			continue
		} else {
			columns = append(columns, filed)
		}
	}
	err := e.CreateSTable(info.DB, info.STableName, &TableInfo{
		Fields: columns,
		Tags:   tags,
	})
	if err != nil {
		//返回错误
		var tdErr *tErrors.TaosError
		if errors.As(err, &tdErr) {
			switch tdErr.Code {
			case tErrors.MND_TABLE_ALREADY_EXIST:
				//表已经创建,返回正常
			default:
				return tdErr
			}
		} else {
			return err
		}
	}
	return nil
}

func (e *Executor) CreateSTable(db string, tableName string, info *TableInfo) error {
	fields := info.Fields
	tags := info.Tags
	if len(fields) == 0 {
		return errors.New("need fields info")
	}
	if len(tags) == 0 {
		return errors.New("need tags info")
	}
	fieldSqlList := []string{"ts timestamp"}
	for _, field := range fields {
		fieldSqlList = append(fieldSqlList, e.generateFieldSql(field))
	}
	var tagsSqlList []string
	for _, tag := range tags {
		tagsSqlList = append(tagsSqlList, e.generateFieldSql(tag))
	}
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("create stable if not exists ")
	b.WriteString(db)
	b.WriteByte('.')
	b.WriteString(tableName)
	b.WriteString(" (")
	b.WriteString(strings.Join(fieldSqlList, ","))
	b.WriteString(") tags (")
	b.WriteString(strings.Join(tagsSqlList, ","))
	b.WriteByte(')')
	err := e.DoExec(b.String())
	return err
}

func (e *Executor) generateFieldSql(info *FieldInfo) string {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	if info.Type == NCHARType || info.Type == BINARYType {
		b.WriteString(info.Name)
		b.WriteByte(' ')
		b.WriteString(info.Type)
		b.WriteByte('(')
		b.WriteString(strconv.Itoa(info.Length))
		b.WriteByte(')')
		return b.String()
	}
	b.WriteString(info.Name)
	b.WriteByte(' ')
	b.WriteString(info.Type)
	return b.String()
}

func (e *Executor) DoExec(sql string) error {
	_, err := e.conn.Exec(sql)
	return err
}

func (e *Executor) createFieldInfo(name string, value interface{}) *FieldInfo {
	if value == nil {
		return nil
	}
	switch value.(type) {
	case float64, float32:
		return &FieldInfo{
			Name: tools.RepairName(name),
			Type: DOUBLEType,
		}
	case int64, int, int8, int16, int32:
		return &FieldInfo{
			Name: tools.RepairName(name),
			Type: BIGINTType,
		}
	case uint64, uint, uint8, uint16, uint32:
		return &FieldInfo{
			Name:   tools.RepairName(name),
			Type:   UBIGINTType,
			Length: 0,
		}
	case string:
		return &FieldInfo{
			Name:   tools.RepairName(name),
			Type:   BINARYType,
			Length: len(value.(string)),
		}
	case bool:
		return &FieldInfo{
			Name:   tools.RepairName(name),
			Type:   BOOLType,
			Length: 0,
		}
	}
	return nil
}
func (e *Executor) modifyTag(db string, stableName string, tableInfo *TableInfo, tagName, tagValue []string) error {

	tagMap := map[string]string{}
	for i := 0; i < len(tagName); i++ {
		tagMap[tools.RepairName(tagName[i])] = tagValue[i]
	}
	modifyLen := map[string]int{}
	for _, tag := range tableInfo.Tags {
		t, exist := tagMap[tag.Name]
		if exist {
			if tag.Type == BINARYType && tag.Length < len(t) {
				modifyLen[tag.Name] = len(t)
			}
		}
		delete(tagMap, tag.Name)
	}
	if len(tagMap) > 0 {
		for tag, tagValue := range tagMap {
			err := e.AddTag(db, stableName, &FieldInfo{
				Name:   tag,
				Type:   BINARYType,
				Length: len(tagValue),
			})
			if err != nil {
				var addTagErr *tErrors.TaosError
				if errors.As(err, &addTagErr) {
					if addTagErr.Code == tErrors.TSC_INVALID_OPERATION {
						continue
					} else {
						return addTagErr
					}
				} else {
					return err
				}
			}
		}
	}
	if len(modifyLen) > 0 {
		for s, i := range modifyLen {
			err := e.ModifyTagLength(db, stableName, &FieldInfo{
				Name:   s,
				Type:   BINARYType,
				Length: i,
			})
			if err != nil {
				var addTagErr *tErrors.TaosError
				if errors.As(err, &addTagErr) {
					if addTagErr.Code == tErrors.TSC_INVALID_TAG_LENGTH {
						continue
					} else {
						return addTagErr
					}
				} else {
					return err
				}
			}
		}
	}
	return nil
}

func (e *Executor) modifyColumn(db string, stableName string, tableInfo *TableInfo, column map[string]interface{}) error {
	columnMap := make(map[string]interface{}, len(column))
	for s, i := range column {
		columnMap[tools.RepairName(s)] = i
	}
	modifyLen := map[string]int{}
	for _, column := range tableInfo.Fields {
		t, exist := columnMap[column.Name]
		if exist {
			if column.Type == BINARYType && column.Length < len(t.(string)) {
				modifyLen[column.Name] = len(t.(string))
			}
		}
		delete(columnMap, column.Name)
	}
	if len(columnMap) > 0 {
		for columnName, columnValue := range columnMap {
			field := e.createFieldInfo(columnName, columnValue)
			if field != nil {
				err := e.AddColumn(db, stableName, field)
				if err != nil {
					var addTagErr *tErrors.TaosError
					if errors.As(err, &addTagErr) {
						if addTagErr.Code == tErrors.TSC_INVALID_OPERATION {
							continue
						} else {
							return addTagErr
						}
					} else {
						return err
					}
				}
			}
		}
	}
	if len(modifyLen) > 0 {
		for s, i := range modifyLen {
			err := e.ModifyColumnLength(db, stableName, &FieldInfo{
				Name:   s,
				Type:   BINARYType,
				Length: i,
			})
			if err != nil {
				var addTagErr *tErrors.TaosError
				if errors.As(err, &addTagErr) {
					if addTagErr.Code == tErrors.TSC_INVALID_COLUMN_LENGTH {
						continue
					} else {
						return addTagErr
					}
				} else {
					return err
				}
			}
		}
	}
	return nil
}

func (e *Executor) DescribeTable(db, tableName string) (*TableInfo, error) {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("describe ")
	b.WriteString(db)
	b.WriteByte('.')
	b.WriteString(tableName)
	rows, err := e.conn.Query(b.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columnNames := rows.Columns()
	var (
		FieldIndex  int
		TypeIndex   int
		LengthIndex int
		NoteIndex   int
	)
	var tags []*FieldInfo
	var fields []*FieldInfo
	for i, s := range columnNames {
		switch s {
		case "Field":
			FieldIndex = i
		case "Type":
			TypeIndex = i
		case "Length":
			LengthIndex = i
		case "Note":
			NoteIndex = i
		}
	}
	for {
		d := make([]driver.Value, len(columnNames))
		err = rows.Next(d)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}
		f := &FieldInfo{
			Name:   d[FieldIndex].(string),
			Type:   d[TypeIndex].(string),
			Length: int(d[LengthIndex].(int32)),
		}
		if d[NoteIndex] == "TAG" {
			tags = append(tags, f)
		} else {
			fields = append(fields, f)
		}
	}

	return &TableInfo{
		Fields: fields,
		Tags:   tags,
	}, nil
}

func (e *Executor) AddTag(db, tableName string, info *FieldInfo) error {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("alter stable ")
	b.WriteString(db)
	b.WriteByte('.')
	b.WriteString(tableName)
	b.WriteString(" add tag ")
	b.WriteString(e.generateFieldSql(info))
	err := e.DoExec(b.String())
	return err
}
func (e *Executor) AddColumn(db, tableName string, info *FieldInfo) error {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("alter stable ")
	b.WriteString(db)
	b.WriteByte('.')
	b.WriteString(tableName)
	b.WriteString(" add column ")
	b.WriteString(e.generateFieldSql(info))
	err := e.DoExec(b.String())
	return err
}
func (e *Executor) ModifyTagLength(db, tableName string, info *FieldInfo) error {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("alter stable ")
	b.WriteString(db)
	b.WriteByte('.')
	b.WriteString(tableName)
	b.WriteString(" modify TAG ")
	b.WriteString(e.generateFieldSql(info))
	err := e.DoExec(b.String())
	return err
}

func (e *Executor) ModifyColumnLength(db, tableName string, info *FieldInfo) error {
	b := pool.BytesPoolGet()
	defer pool.BytesPoolPut(b)
	b.WriteString("alter stable ")
	b.WriteString(db)
	b.WriteByte('.')
	b.WriteString(tableName)
	b.WriteString(" modify COLUMN ")
	b.WriteString(e.generateFieldSql(info))
	err := e.DoExec(b.String())
	return err
}

type TableInfo struct {
	Fields []*FieldInfo
	Tags   []*FieldInfo
}
type FieldInfo struct {
	Name   string
	Type   string
	Length int
}

func (e *Executor) generateInsertSql(line *InsertLine) string {
	b := pool.BytesPoolGet()
	b.WriteString("insert into ")
	b.WriteString(line.DB)
	b.WriteByte('.')
	b.WriteString(line.TableName)
	b.WriteString(" using ")
	b.WriteString(line.DB)
	b.WriteByte('.')
	b.WriteString(line.STableName)
	if len(line.TagNames) > 0 {
		b.WriteString(" (")
		for i, name := range line.TagNames {
			b.WriteString(tools.RepairName(name))
			if i != len(line.TagNames)-1 {
				b.WriteByte(',')
			}
		}
		b.WriteString(") tags(")
		for i, value := range line.TagValues {
			b.WriteByte('\'')
			b.WriteString(value)
			b.WriteByte('\'')
			if i != len(line.TagValues)-1 {
				b.WriteByte(',')
			}
		}
		b.WriteByte(')')
	}
	b.WriteString(" (ts")
	values := make([]interface{}, 0, len(line.Fields))
	for k, v := range line.Fields {
		if v == nil {
			continue
		}
		b.WriteByte(',')
		b.WriteString(tools.RepairName(k))
		values = append(values, v)
	}
	b.WriteString(") values('")
	b.WriteString(line.Ts.Format(time.RFC3339Nano))
	b.WriteByte('\'')
	for _, value := range values {
		b.WriteByte(',')
		if value == nil {
			b.WriteString("null")
		} else {
			switch v := value.(type) {
			case int:
				b.WriteString(strconv.FormatInt(int64(v), 10))
			case int8:
				b.WriteString(strconv.FormatInt(int64(v), 10))
			case int16:
				b.WriteString(strconv.FormatInt(int64(v), 10))
			case int32:
				b.WriteString(strconv.FormatInt(int64(v), 10))
			case int64:
				b.WriteString(strconv.FormatInt(v, 10))
			case uint:
				b.WriteString(strconv.FormatUint(uint64(v), 10))
			case uint8:
				b.WriteString(strconv.FormatUint(uint64(v), 10))
			case uint16:
				b.WriteString(strconv.FormatUint(uint64(v), 10))
			case uint32:
				b.WriteString(strconv.FormatUint(uint64(v), 10))
			case uint64:
				b.WriteString(strconv.FormatUint(v, 10))
			case float64:
				b.WriteString(strconv.FormatFloat(v, 'f', -1, 64))
			case float32:
				b.WriteString(strconv.FormatFloat(float64(v), 'f', -1, 32))
			case bool:
				if v {
					b.WriteString("true")
				} else {
					b.WriteString("false")
				}
			case string:
				b.WriteByte('\'')
				b.WriteString(v)
				b.WriteByte('\'')
			}
		}
	}
	b.WriteByte(')')
	return b.String()
}

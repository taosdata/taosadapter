package inputjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/blues/jsonata-go"
	"github.com/gin-gonic/gin"
	"github.com/ncruces/go-strftime"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/async"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
	errors2 "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/plugin"
	"github.com/taosdata/taosadapter/v3/tools/connectpool"
	"github.com/taosdata/taosadapter/v3/tools/generator"
	"github.com/taosdata/taosadapter/v3/tools/iptool"
)

const MAXSQLLength = 1024 * 1024 * 1

var logger = log.GetLogger("PLG").WithField("mod", "input_json")

type Plugin struct {
	conf  Config
	rules map[string]*ParsedRule
}

type ParsedRule struct {
	TransformationExpr *jsonata.Expr
	DB                 string
	DBKey              string
	SuperTable         string
	SuperTableKey      string
	SubTable           string
	SubTableKey        string
	TimeKey            string
	TimeFormat         string
	TimeTimezone       *time.Location
	TimeFieldName      string
	SqlAllColumns      string
	FieldKeys          []string
	FieldOptionals     []bool
}

func (p *Plugin) Init(r gin.IRouter) error {
	err := p.conf.setValue(viper.GetViper())
	if err != nil {
		return err
	}
	if !p.conf.Enable {
		logger.Debug("input_json disabled")
		return nil
	}
	err = p.conf.validate()
	if err != nil {
		return err
	}
	p.rules = make(map[string]*ParsedRule, len(p.conf.Rules))
	builder := &strings.Builder{}
	for _, rule := range p.conf.Rules {
		if rule.Transformation != "" {
			// compile jsonata expression
			e, err := jsonata.Compile(rule.Transformation)
			if err != nil {
				return fmt.Errorf("error compiling jsonata expression: %v, expr: %s", err, rule.Transformation)
			}
			format := rule.TimeFormat
			if !isPredefinedTimeFormat(rule.TimeFormat) {
				// convert strftime format
				format, err = strftime.Layout(rule.TimeFormat)
				if err != nil {
					return fmt.Errorf("error compiling strftime time format: %v, format: %s", err, rule.TimeFormat)
				}
			}
			loc := time.Local
			if rule.TimeTimezone != "" {
				// load location
				loc, err = time.LoadLocation(rule.TimeTimezone)
				if err != nil {
					return fmt.Errorf("error loading time location: %v, timezone: %s", err, rule.TimeTimezone)
				}
			}
			fieldKeys := make([]string, len(rule.Fields))
			fieldOptionals := make([]bool, len(rule.Fields))
			for i, field := range rule.Fields {
				fieldKeys[i] = field.Key
				fieldOptionals[i] = field.Optional
			}

			parsedRule := &ParsedRule{
				TransformationExpr: e,
				DB:                 rule.DB,
				DBKey:              rule.DBKey,
				SuperTable:         rule.SuperTable,
				SuperTableKey:      rule.SuperTableKey,
				SubTable:           rule.SubTable,
				SubTableKey:        rule.SubTableKey,
				TimeKey:            rule.TimeKey,
				TimeFormat:         format,
				TimeTimezone:       loc,
				TimeFieldName:      rule.TimeFieldName,
				FieldKeys:          fieldKeys,
				FieldOptionals:     fieldOptionals,
			}
			builder.Reset()
			generateFieldKeyStatement(builder, parsedRule, len(rule.Fields))
			parsedRule.SqlAllColumns = builder.String()
			builder.Reset()
			p.rules[rule.Endpoint] = parsedRule
		}
	}
	r.POST(":endpoint", plugin.Auth(errorResponse), p.HandleRequest)
	return nil
}

type record struct {
	incompleteValue bool
	stb             string
	subTable        string
	ts              time.Time
	keys            string
	values          string
}
type dryRunResp struct {
	Code int      `json:"code"`
	Desc string   `json:"desc"`
	Json string   `json:"json"`
	Sql  []string `json:"sql"`
}

type message struct {
	Code     int    `json:"code"`
	Desc     string `json:"desc,omitempty"`
	Affected int    `json:"affected,omitempty"`
}

func errorResponse(c *gin.Context, code int, err error) {
	errorRespWithErrorCode(c, code, 0xffff, err.Error())
}

func errorRespWithErrorCode(c *gin.Context, httpCode int, errCode int, desc string) {
	c.JSON(httpCode, message{
		Code: errCode,
		Desc: desc,
	})
}

func successResponse(c *gin.Context, affectedRows int) {
	c.JSON(http.StatusOK, message{
		Code:     http.StatusOK,
		Affected: affectedRows,
	})
}

func (p *Plugin) HandleRequest(c *gin.Context) {
	endpoint := c.Param("endpoint")
	rule, ok := p.rules[endpoint]
	if !ok {
		errorResponse(c, http.StatusNotFound, fmt.Errorf("no rule found for endpoint: %s", endpoint))
		return
	}
	var reqID int64
	var err error
	if reqIDStr := c.Query("req_id"); len(reqIDStr) != 0 {
		if reqID, err = strconv.ParseInt(reqIDStr, 10, 64); err != nil {
			logger.Errorf("illegal param, req_id must be numeric:%s, err:%s", reqIDStr, err)
			errorResponse(c, http.StatusBadRequest, fmt.Errorf("illegal param, req_id must be numeric %s", err.Error()))
			return
		}
	}
	if reqID == 0 {
		reqID = generator.GetReqID()
		logger.Tracef("request:%s, client_ip:%s, req_id not set, generate new QID:0x%x", c.Request.RequestURI, c.ClientIP(), reqID)
	}
	logger := logger.WithField(config.ReqIDKey, reqID)
	isDebug := log.IsDebug()
	dryRun := c.Query("dry_run") == "true"
	if dryRun {
		logger.Debug("dry_run mode enabled, no data will be inserted")
	}
	user, password, err := plugin.GetAuth(c)
	if err != nil {
		logger.Errorf("get user and password error:%s", err.Error())
		errorResponse(c, http.StatusUnauthorized, fmt.Errorf("get user and password error:%s", err.Error()))
		return
	}
	s := log.GetLogNow(isDebug)
	taosConn, err := commonpool.GetConnection(user, password, iptool.GetRealIP(c.Request))
	logger.Debugf("get connection finish, cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.Errorf("get connection from pool error, err:%s", err)
		if errors.Is(err, commonpool.ErrWhitelistForbidden) {
			errorResponse(c, http.StatusForbidden, fmt.Errorf("whitelist forbidden: %s", err.Error()))
			return
		}
		if errors.Is(err, connectpool.ErrTimeout) || errors.Is(err, connectpool.ErrMaxWait) {
			errorResponse(c, http.StatusGatewayTimeout, fmt.Errorf("get connect from pool error: %s", err.Error()))
			return
		}
		errorResponse(c, http.StatusInternalServerError, fmt.Errorf("get connect from pool error: %s", err.Error()))
		return
	}
	if dryRun {
		logger.Tracef("put connection")
		_ = taosConn.Put()
	} else {
		defer func() {
			logger.Tracef("put connection")
			putErr := taosConn.Put()
			if putErr != nil {
				logger.Errorf("connect pool put error, err:%s", putErr)
			}
		}()
	}
	// transform json payload if transformation is defined
	var jsonData []byte
	jsonData, err = c.GetRawData()
	if err != nil {
		errorResponse(c, http.StatusBadRequest, fmt.Errorf("error reading request body: %v", err))
		return
	}
	if rule.TransformationExpr != nil {
		var v interface{}
		// precision loss may occur here
		// e.g. large int64 may be converted to float64
		// it's recommended to use string for large int64 values in json
		err := json.Unmarshal(jsonData, &v)
		if err != nil {
			errorResponse(c, http.StatusBadRequest, fmt.Errorf("error unmarshaling json data: %v", err))
			return
		}

		v, err = rule.TransformationExpr.Eval(v)
		if err != nil {
			errorResponse(c, http.StatusBadRequest, fmt.Errorf("error evaluating jsonata expression: %v", err))
			return
		}
		jsonData, err = rule.TransformationExpr.EvalBytes(jsonData)
		if err != nil {
			errorResponse(c, http.StatusBadRequest, fmt.Errorf("error evaluating jsonata expression: %v", err))
			return
		}
	}
	// parse jsonData according to rule
	if jsonData == nil {
		errorResponse(c, http.StatusBadRequest, fmt.Errorf("no data after transformation"))
		return
	}
	records, err := parseRecord(rule, jsonData, logger)
	if err != nil {
		errorResponse(c, http.StatusBadRequest, fmt.Errorf("error parsing records: %v", err))
		return
	}
	sqlArray := generateSql(records)
	if dryRun {
		resp := dryRunResp{
			Json: string(jsonData),
			Sql:  sqlArray,
		}
		c.JSON(200, resp)
	} else {
		affectedRows, err := execute(taosConn.TaosConnection, reqID, sqlArray, logger, isDebug)
		if err != nil {
			httpRespCode := http.StatusInternalServerError
			errorCode := 0xffff
			taosErr, ok := err.(*errors2.TaosError)
			if ok {
				respCode, exists := config.ErrorStatusMap[taosErr.Code]
				if exists {
					httpRespCode = respCode
				}
				errorCode = int(taosErr.Code)
			}
			errorRespWithErrorCode(c, httpRespCode, errorCode, err.Error())
			return
		}
		successResponse(c, affectedRows)
	}
}

func parseRecord(rule *ParsedRule, jsonData []byte, logger *logrus.Entry) ([]*record, error) {
	var result []map[string]interface{}
	err := Unmarshal(jsonData, &result)
	if err != nil {
		logger.Errorf("error unmarshaling json data:%s, err:%s", string(jsonData), err.Error())
		return nil, fmt.Errorf("error unmarshaling json data: %v", err)
	}
	records := make([]*record, len(result))
	fieldBuilder := &strings.Builder{}
	valueBuilder := &strings.Builder{}
	timeBuf := make([]byte, 0, 35)
	for i, item := range result {
		rec := &record{}
		// parse DB
		var db string
		if rule.DBKey != "" {
			v, ok := item[rule.DBKey]
			if !ok {
				logger.Errorf("db key %s not found in item: %v", rule.DBKey, item)
				return nil, fmt.Errorf("db key %s not found in item", rule.DBKey)
			}
			db, err = castToString(v)
			if err != nil {
				return nil, err
			}
			db = escapeIdentifier(db)
		} else {
			db = rule.DB
		}
		// parse SuperTable
		var stb string
		if rule.SuperTableKey != "" {
			v, ok := item[rule.SuperTableKey]
			if !ok {
				logger.Errorf("super_table key %s not found in item: %v", rule.SuperTableKey, item)
				return nil, fmt.Errorf("super table key %s not found in item", rule.SuperTableKey)
			}
			stb, err = castToString(v)
			if err != nil {
				return nil, err
			}
			stb = escapeIdentifier(stb)
		} else {
			stb = rule.SuperTable
		}
		rec.stb = fmt.Sprintf("`%s`.`%s`", db, stb)
		// parse SubTable
		if rule.SubTableKey != "" {
			v, ok := item[rule.SubTableKey]
			if !ok {
				logger.Errorf("sub_table key %s not found in item: %v", rule.SubTableKey, item)
				return nil, fmt.Errorf("sub table key %s not found in item", rule.SubTableKey)
			}
			rec.subTable, err = castToString(v)
			if err != nil {
				return nil, err
			}
		} else {
			rec.subTable = rule.SubTable
		}
		// tableName part
		// e.g. 'sub_table_1'
		valueBuilder.Reset()
		valueBuilder.WriteByte('\'')
		valueBuilder.WriteString(escapeStringValue(rec.subTable))
		// parse Time
		if rule.TimeKey != "" {
			v, ok := item[rule.TimeKey]
			if !ok {
				logger.Errorf("time key %s not found in item: %v", rule.TimeKey, item)
				return nil, fmt.Errorf("time key %s not found in item", rule.TimeKey)
			}
			val, err := castToString(v)
			if err != nil {
				return nil, err
			}
			rec.ts, err = parseTime(val, rule.TimeFormat, rule.TimeTimezone)
			if err != nil {
				logger.Errorf("parse time error:%s, format:%s, err:%s", val, rule.TimeFormat, err.Error())
				return nil, err
			}
		} else {
			rec.ts = time.Now()
		}
		// time part
		// e.g. ,'2024-06-10T15:04:05.999999999Z'
		valueBuilder.WriteString(",'")
		timeBuf = timeBuf[:0]
		timeBuf = rec.ts.AppendFormat(timeBuf, time.RFC3339Nano)
		valueBuilder.WriteByte('\'')
		valueBuilder.Write(timeBuf)
		valueBuilder.WriteByte('\'')
		// parse Fields
		inCompleteValue := false
		for index, fieldKey := range rule.FieldKeys {
			v, ok := item[fieldKey]
			if !ok {
				if rule.FieldOptionals[index] {
					if inCompleteValue {
						writeFieldName(fieldBuilder, fieldKey)
					} else {
						fieldBuilder.Reset()
						generateFieldKeyStatement(fieldBuilder, rule, index)
						inCompleteValue = true
					}
				} else {
					logger.Errorf("field key %s not found in item: %v", fieldKey, item)
					return nil, fmt.Errorf("field key %s not found in json", fieldKey)
				}
			}
			err = writeValue(valueBuilder, v)
			if err != nil {
				logger.Errorf("write field error:%s, field:%s, err:%s", fieldKey, fieldKey, err.Error())
				return nil, err
			}
		}
		rec.incompleteValue = inCompleteValue
		if inCompleteValue {
			rec.keys = fieldBuilder.String()
			fieldBuilder.Reset()
		} else {
			rec.keys = rule.SqlAllColumns
		}
		rec.values = valueBuilder.String()
		valueBuilder.Reset()
		records[i] = rec
	}
	return records, nil
}

func castToString(value interface{}) (string, error) {
	if value == nil {
		return "null", nil
	}
	switch v := value.(type) {
	case string:
		return v, nil
	case json.Number:
		return v.String(), nil
	case bool:
		return fmt.Sprintf("%v", v), nil
	default:
		return "", fmt.Errorf("unsupported value type: %T,value: %v", value, value)
	}
}

func writeValue(builder *strings.Builder, value interface{}) error {
	if value == nil {
		builder.WriteString("null")
		return nil
	}
	switch v := value.(type) {
	case string:
		builder.WriteByte('\'')
		builder.WriteString(escapeStringValue(v))
		builder.WriteByte('\'')
	case json.Number:
		builder.WriteString(v.String())
	case bool:
		if v {
			builder.WriteString("true")
		} else {
			builder.WriteString("false")
		}
	default:
		return fmt.Errorf("unsupported value type: %T,value: %v", value, value)
	}
	return nil
}

func escapeStringValue(s string) string {
	builder := strings.Builder{}
	for _, v := range s {
		switch v {
		case 0:
			continue
		case '\n':
			builder.WriteString(`\n`)
		case '\r':
			builder.WriteString(`\r`)
		case '\t':
			builder.WriteString(`\t`)
		case '\'':
			builder.WriteString(`''`)
		case '"':
			builder.WriteString(`\"`)
		case '\\':
			builder.WriteString(`\\`)
		default:
			builder.WriteRune(v)
		}
	}
	return builder.String()
}

// escapeIdentifier escapes backticks in identifiers
// e.g. table`name -> table“name
func escapeIdentifier(identifier string) string {
	return strings.ReplaceAll(identifier, "`", "``")
}

const InsertSqlStatement = "insert into "

func generateSql(records []*record) []string {
	slices.SortFunc(records, func(a, b *record) int {
		// sorting by stb, subTable, keys, ts
		if a.stb != b.stb {
			return strings.Compare(a.stb, b.stb)
		}
		if a.subTable != b.subTable {
			return strings.Compare(a.subTable, b.subTable)
		}
		if a.keys != b.keys {
			return strings.Compare(a.keys, b.keys)
		}
		return a.ts.Compare(b.ts)
	})
	sqlBuilder := &strings.Builder{}
	tmpBuilder := &bytes.Buffer{}
	var sqlArray []string
	lastStb := ""
	lastKey := ""
	containsStb := false
	sqlBuilder.WriteString(InsertSqlStatement)
	for i := 0; i < len(records); i++ {
		rec := records[i]
		// new supertable or different field keys
		// need to write supertable part
		if rec.stb != lastStb || rec.keys != lastKey {
			writeSuperTableStatement(tmpBuilder, rec.stb, rec.keys)
			lastStb = rec.stb
			lastKey = rec.keys
			containsStb = true
		} else {
			containsStb = false
		}
		// write values part
		tmpBuilder.WriteByte('(')
		tmpBuilder.WriteString(rec.values)
		tmpBuilder.WriteByte(')')
		if sqlBuilder.Len()+tmpBuilder.Len() >= MAXSQLLength {
			// flush current sql
			sqlArray = append(sqlArray, sqlBuilder.String())
			sqlBuilder.Reset()
			sqlBuilder.WriteString(InsertSqlStatement)
			if !containsStb {
				// need to write supertable part
				writeSuperTableStatement(sqlBuilder, rec.stb, rec.keys)
			}
		}
		sqlBuilder.Write(tmpBuilder.Bytes())
		tmpBuilder.Reset()
	}
	if sqlBuilder.Len() > len(InsertSqlStatement) {
		sqlArray = append(sqlArray, sqlBuilder.String())
	}
	return sqlArray
}

// writeSuperTableStatement writes the insert into supertable part of the insert statement
// e.g. `db`.`stb` (`tbname`,`ts`,`field1`,`field2`) values
func writeSuperTableStatement(builder io.StringWriter, superTable string, keys string) {
	// strings.Builder and bytes.Buffer will not return error on WriteString
	_, _ = builder.WriteString(superTable)
	_, _ = builder.WriteString(" (`tbname`,")
	_, _ = builder.WriteString(keys)
	_, _ = builder.WriteString(") values")
}

// generateFieldKeyStatement generates the field key part of the insert statement
// e.g. `ts`,`field1`,`field2`
func generateFieldKeyStatement(builder *strings.Builder, rule *ParsedRule, count int) {
	builder.WriteByte('`')
	builder.WriteString(rule.TimeFieldName)
	builder.WriteByte('`')
	for i := 0; i < count; i++ {
		writeFieldName(builder, rule.FieldKeys[i])
	}
}

// writeFieldName writes a field name to the builder with proper formatting
// e.g. ,`fieldName`
func writeFieldName(builder *strings.Builder, fieldKey string) {
	builder.WriteString(",`")
	builder.WriteString(fieldKey)
	builder.WriteByte('`')
}

// execute executes the given SQL statements and returns the total affected rows
func execute(conn unsafe.Pointer, reqID int64, sqlArray []string, logger *logrus.Entry, isDebug bool) (int, error) {
	totalAffectedRows := 0
	for i := 0; i < len(sqlArray); i++ {
		affectedRows, err := async.GlobalAsync.TaosExecWithAffectedRows(conn, logger, isDebug, sqlArray[i], reqID)
		if err != nil {
			logger.Errorf("error executing sql: %v, sql: %s", err, sqlArray[i])
			return 0, err
		}
		totalAffectedRows += affectedRows
	}
	return totalAffectedRows, nil
}

func (p *Plugin) Start() error {
	return nil
}

func (p *Plugin) Stop() error {
	return nil
}

func (p *Plugin) String() string {
	return "input_json"
}

func (p *Plugin) Version() string {
	return "v1"
}

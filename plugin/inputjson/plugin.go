package inputjson

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blues/jsonata-go"
	"github.com/gin-gonic/gin"
	"github.com/ncruces/go-strftime"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/plugin"
)

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
	r.POST(":endpoint", plugin.Auth(p.errorResponse), p.HandleRequest)
	return nil
}

type record struct {
	incompleteValue bool
	rule            *ParsedRule
	stb             string
	subTable        string
	ts              time.Time
	keys            string
	values          string
}

func (p *Plugin) HandleRequest(c *gin.Context) {
	endpoint := c.Param("endpoint")
	rule, ok := p.rules[endpoint]
	if !ok {
		p.errorResponse(c, 404, fmt.Errorf("no rule found for endpoint: %s", endpoint))
		return
	}
	// transform json payload if transformation is defined
	var jsonData []byte
	jsonData, err := c.GetRawData()
	if err != nil {
		p.errorResponse(c, 500, fmt.Errorf("error reading request body: %v", err))
		return
	}
	if rule.TransformationExpr != nil {
		var v interface{}
		// precision loss may occur here
		// e.g. large int64 may be converted to float64
		// it's recommended to use string for large int64 values in json
		err := json.Unmarshal(jsonData, &v)
		if err != nil {
			p.errorResponse(c, 400, fmt.Errorf("error unmarshaling json data: %v", err))
			return
		}

		v, err = rule.TransformationExpr.Eval(v)
		if err != nil {
			p.errorResponse(c, 400, fmt.Errorf("error evaluating jsonata expression: %v", err))
			return
		}
		jsonData, err = rule.TransformationExpr.EvalBytes(jsonData)
		if err != nil {
			p.errorResponse(c, 400, fmt.Errorf("error evaluating jsonata expression: %v", err))
			return
		}
	}
	// parse jsonData according to rule
	if jsonData == nil {
		p.errorResponse(c, 400, fmt.Errorf("no data after transformation"))
		return
	}
	records, err := parseRecord(rule, jsonData, logger)
	if err != nil {
		p.errorResponse(c, 400, fmt.Errorf("error parsing records: %v", err))
		return
	}

}

func parseRecord(rule *ParsedRule, jsonData []byte, logger *logrus.Entry) ([]*record, error) {
	var result []map[string]interface{}
	err := json.Unmarshal(jsonData, &result)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling json data: %v", err)
	}
	records := make([]*record, len(result))
	fieldBuilder := &strings.Builder{}
	valueBuilder := &strings.Builder{}
	timeBuf := make([]byte, 0, 35)
	for i, item := range result {
		rec := &record{
			rule: rule,
		}
		// parse DB
		var db string
		if rule.DBKey != "" {
			v, ok := item[rule.DBKey]
			if !ok {
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
		valueBuilder.WriteString(rec.subTable)
		// parse Time
		if rule.TimeKey != "" {
			v, ok := item[rule.TimeKey]
			if !ok {
				return nil, fmt.Errorf("time key %s not found in item", rule.TimeKey)
			}
			val, err := castToString(v)
			if err != nil {
				return nil, err
			}
			rec.ts, err = parseTime(val, rule.TimeFormat, rule.TimeTimezone)
			if err != nil {
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
					return nil, fmt.Errorf("field key %s not found in json", fieldKey)
				}
			}
			err = writeValue(valueBuilder, v)
			if err != nil {
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

func escapeIdentifier(identifier string) string {
	return strings.ReplaceAll(identifier, "`", "``")
}

const insertSqlTemplate = "insert into %s (`tbname`%s) values(%s)"

func GenerateSql(records []*record) []string {
	sort.Slice(records, func(i, j int) bool {
		// sorting by stb, subTable, ts
		if records[i].stb != records[j].stb {
			return records[i].stb < records[j].stb
		}
		if records[i].subTable != records[j].subTable {
			return records[i].subTable < records[j].subTable
		}
		return records[i].ts.Before(records[j].ts)
	})
	sqlBuilder := &strings.Builder{}
	tmpBuilder := &bytes.Buffer{}
	//todo

	//var sqls []string
	//lastStb := ""
	//sqlBuilder.WriteString("insert into ")
	//for i := 0; i < len(records); i++ {
	//	rec := records[i]
	//	if rec.stb != lastStb {
	//		tmpBuilder.WriteString()
	//		lastStb = rec.stb
	//		tmpBuilder.Reset()
	//	}
	//}
	return nil
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

type message struct {
	Code    int    `json:"code"`
	Message string `json:"message,omitempty"`
}

func (p *Plugin) errorResponse(c *gin.Context, code int, err error) {
	c.JSON(code, message{
		Code:    code,
		Message: err.Error(),
	})
}

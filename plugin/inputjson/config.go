package inputjson

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	Enable bool
	Rules  []*Rule
}

type Rule struct {
	Endpoint       string
	DB             string
	DBKey          string
	SuperTable     string
	SuperTableKey  string
	SubTable       string
	SubTableKey    string
	TimeKey        string
	TimeFormat     string
	TimeTimezone   string
	TimeFieldName  string
	Transformation string
	Fields         []*Field
}

type Field struct {
	Key      string
	Optional bool
}

func (c *Config) setValue(v *viper.Viper) error {
	c.Enable = v.GetBool("input_json.enable")
	if c.Enable == false {
		return nil
	}
	// if enabled , rules must be provided
	keys := v.AllKeys()
	fmt.Println("keys:", keys)
	err := v.Unmarshal(c)
	if err != nil {
		return err
	}
	rules := v.Get("input_json.rules")
	rulesList, ok := rules.([]interface{})
	if !ok {
		return fmt.Errorf("input_json.rules is not valid")
	}
	if len(rulesList) == 0 {
		return fmt.Errorf("input_json.rules is empty")
	}
	for _, r := range rulesList {
		r, ok := r.(map[string]interface{})
		if !ok {
			return fmt.Errorf("input_json.rules item is not valid")
		}
		rule := &Rule{}
		rule.Endpoint, err = getStringConfig(r, "endpoint", "")
		if err != nil {
			return err
		}
		rule.DB, err = getStringConfig(r, "db", "")
		if err != nil {
			return err
		}
		rule.DBKey, err = getStringConfig(r, "dbKey", "")
		if err != nil {
			return err
		}
		rule.SuperTable, err = getStringConfig(r, "superTable", "")
		if err != nil {
			return err
		}
		rule.SuperTableKey, err = getStringConfig(r, "superTableKey", "")
		if err != nil {
			return err
		}
		rule.SubTable, err = getStringConfig(r, "subTable", "")
		if err != nil {
			return err
		}
		rule.SubTableKey, err = getStringConfig(r, "subTableKey", "")
		if err != nil {
			return err
		}
		rule.TimeKey, err = getStringConfig(r, "timeKey", "")
		if err != nil {
			return err
		}
		rule.TimeFormat, err = getStringConfig(r, "timeFormat", "")
		if err != nil {
			return err
		}
		rule.TimeTimezone, err = getStringConfig(r, "timeTimezone", "")
		if err != nil {
			return err
		}
		rule.Transformation, err = getStringConfig(r, "transformation", "")
		if err != nil {
			return err
		}

		rule.TimeFieldName, err = getStringConfig(r, "timeFieldName", "ts")
		if err != nil {
			return err
		}

		if fieldList, exist := r["fields"].([]interface{}); exist {
			for _, tag := range fieldList {
				tagMap, ok := tag.(map[string]interface{})
				if !ok {
					return fmt.Errorf("input_json.rules.fields item is not valid: %v", tag)
				}
				field, err := parseFiled(tagMap)
				if err != nil {
					return err
				}
				rule.Fields = append(rule.Fields, field)
			}
		}
		c.Rules = append(c.Rules, rule)
	}
	return nil
}

func getStringConfig(mapData map[string]interface{}, key string, defaultVal string) (string, error) {
	k := strings.ToLower(key)
	value, exist := mapData[k]
	if !exist {
		return defaultVal, nil
	}
	strValue, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%s is not string, value: %v", key, value)
	}
	return strValue, nil
}

func getBoolConfig(mapData map[string]interface{}, key string) (bool, error) {
	k := strings.ToLower(key)
	value, exist := mapData[k]
	if !exist {
		return false, nil
	}
	boolValue, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("%s is not bool, value: %v", key, value)
	}
	return boolValue, nil
}

func parseFiled(mapData map[string]interface{}) (*Field, error) {
	field := &Field{}
	var err error
	field.Key, err = getStringConfig(mapData, "key", "")
	if err != nil {
		return nil, err
	}
	field.Optional, err = getBoolConfig(mapData, "optional")
	if err != nil {
		return nil, err
	}
	return field, nil
}

func (c *Config) validate() error {
	if c.Enable == false {
		return nil
	}
	if len(c.Rules) == 0 {
		return fmt.Errorf("input_json.rules is empty")
	}
	for _, rule := range c.Rules {
		if rule.Endpoint == "" {
			return fmt.Errorf("input_json.rules.endpoint is empty")
		}
		if !isValidEndpoint(rule.Endpoint) {
			return fmt.Errorf("input_json.rules.endpoint:%s is not valid", rule.Endpoint)
		}
		if rule.DB == "" && rule.DBKey == "" {
			return fmt.Errorf("endpoint:%s db and dbKey are empty", rule.Endpoint)
		}
		if rule.SuperTable == "" && rule.SuperTableKey == "" {
			return fmt.Errorf("endpoint:%s superTable and superTableKey are empty", rule.Endpoint)
		}
		if rule.SubTable == "" && rule.SubTableKey == "" {
			return fmt.Errorf("endpoint:%s subTable and subTableKey are empty", rule.Endpoint)
		}
		if rule.TimeKey == "" {
			return fmt.Errorf("endpoint:%s timeKey is empty", rule.Endpoint)
		}
		if rule.TimeFormat == "" {
			return fmt.Errorf("endpoint:%s timeFormat is empty", rule.Endpoint)
		}
		if !isValidIdentifier(rule.DB) {
			return fmt.Errorf("endpoint:%s db:%s is not valid", rule.Endpoint, rule.DB)
		}
		if !isValidIdentifier(rule.SuperTable) {
			return fmt.Errorf("endpoint:%s superTable:%s is not valid", rule.Endpoint, rule.SuperTable)
		}
		if !isValidIdentifier(rule.SubTable) {
			return fmt.Errorf("endpoint:%s subTable:%s is not valid", rule.Endpoint, rule.SubTable)
		}
		if len(rule.Fields) == 0 {
			return fmt.Errorf("endpoint:%s fields is empty", rule.Endpoint)
		}
		fieldKeyMap := make(map[string]struct{}, len(rule.Fields))
		for _, field := range rule.Fields {
			if field.Key == "" {
				return fmt.Errorf("endpoint:%s fields contains empty key", rule.Endpoint)
			}
			if !isValidIdentifier(field.Key) {
				return fmt.Errorf("endpoint:%s fields key:%s is not valid", rule.Endpoint, field.Key)
			}
			if _, exist := fieldKeyMap[field.Key]; exist {
				return fmt.Errorf("endpoint:%s fields contains duplicate key:%s", rule.Endpoint, field.Key)
			}
			fieldKeyMap[field.Key] = struct{}{}
		}
	}
	return nil
}

func isValidEndpoint(endpoint string) bool {
	if len(endpoint) == 0 {
		return false
	}

	for _, char := range endpoint {
		if !(char >= 'a' && char <= 'z') &&
			!(char >= 'A' && char <= 'Z') &&
			!(char >= '0' && char <= '9') {
			return false
		}
	}
	return true
}

func isValidIdentifier(name string) bool {
	if name == "" {
		return true
	}
	if strings.Contains(name, "`") {
		return false
	}
	return true
}
func initViper(v *viper.Viper) {
	v.SetDefault("input_json.enable", true)
}

func init() {
	initViper(viper.GetViper())
}

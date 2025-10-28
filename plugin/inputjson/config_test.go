package inputjson

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_setValue(t *testing.T) {
	tmpDir := t.TempDir()
	type fields struct {
		Enable bool
		Rules  []*Rule
	}
	type args struct {
		v *viper.Viper
	}
	tests := []struct {
		name    string
		content string
		fields  fields
		wantErr bool
	}{
		{
			name: "valid config",
			content: `
[input_json]
enable = true
# rules1
[[input_json.rules]]
endpoint = "rule1"
db = "inputdb"
superTable = "supertable1"
subTable = "subtable1"
timeKey = "ts"
timeFormat = "iso8601nano"
TimeTimezone = "Asia/Shanghai"
tag = [
    {key = "Key1", optional = false},
    {key = "Key2", optional = true}
]
col = [
    {key = "Key3", optional = false}
]

# rules2
[[input_json.rules]]
endpoint = "rule2"
db = "inputdb2"
superTable = "supertable2"
subTable = "subtable2"
timeKey = "timestamp"
timeFormat = "unix"
TimeTimezone = "UTC"
tag = [
    {key = "TagA", optional = false},
    {key = "TagB", optional = false}
]
col = [
    {key = "ColX", optional = true},
    {key = "ColY", optional = false}
]
`,
			fields: fields{
				Enable: true,
				Rules: []*Rule{
					{
						Endpoint:     "rule1",
						DB:           "inputdb",
						SuperTable:   "supertable1",
						SubTable:     "subtable1",
						TimeKey:      "ts",
						TimeFormat:   "iso8601nano",
						TimeTimezone: "Asia/Shanghai",
						Tag: []*Field{
							{Key: "Key1", Optional: false},
							{Key: "Key2", Optional: true},
						},
						Col: []*Field{
							{Key: "Key3", Optional: false},
						},
					},
					{
						Endpoint:     "rule2",
						DB:           "inputdb2",
						SuperTable:   "supertable2",
						SubTable:     "subtable2",
						TimeKey:      "timestamp",
						TimeFormat:   "unix",
						TimeTimezone: "UTC",
						Tag: []*Field{
							{Key: "TagA", Optional: false},
							{Key: "TagB", Optional: false},
						},
						Col: []*Field{
							{Key: "ColX", Optional: true},
							{Key: "ColY", Optional: false},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configPath := filepath.Join(tmpDir, "config.toml")
			err := os.WriteFile(configPath, []byte(tt.content), 0644)
			require.NoError(t, err)
			v := viper.New()
			v.SetConfigType("toml")
			v.SetConfigFile(configPath)
			err = v.ReadInConfig()
			require.NoError(t, err)
			want := &Config{
				Enable: tt.fields.Enable,
				Rules:  tt.fields.Rules,
			}
			got := &Config{}
			if err := got.setValue(v); (err != nil) != tt.wantErr {
				t.Errorf("setValue() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.Equal(t, want, got)
		})
	}
}

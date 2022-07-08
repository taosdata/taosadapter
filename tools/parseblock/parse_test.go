package parseblock

import (
	"database/sql/driver"
	"reflect"
	"testing"

	"github.com/taosdata/driver-go/v3/common"
)

func TestParseBlock(t *testing.T) {
	type args struct {
		data      []byte
		colTypes  []uint8
		rows      int
		precision int
	}
	tests := []struct {
		name  string
		args  args
		want  uint64
		want1 [][]driver.Value
	}{
		{
			name: "",
			args: args{
				data:      []byte{2, 0, 0, 0, 0, 0, 0, 0, 76, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 4, 0, 0, 0, 52, 0, 0, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0},
				colTypes:  []uint8{common.TSDB_DATA_TYPE_INT},
				rows:      13,
				precision: 0,
			},
			want: 2,
			want1: [][]driver.Value{
				{int32(100)}, {int32(100)}, {int32(100)}, {int32(100)}, {int32(100)}, {int32(100)}, {int32(100)}, {int32(100)}, {int32(100)}, {int32(100)}, {int32(100)}, {int32(100)}, {int32(100)},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := ParseBlock(tt.args.data, tt.args.colTypes, tt.args.rows, tt.args.precision)
			if got != tt.want {
				t.Errorf("ParseBlock() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("ParseBlock() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestParseTmqBlock(t *testing.T) {
	type args struct {
		data      []byte
		colTypes  []uint8
		rows      int
		precision int
	}
	tests := []struct {
		name      string
		args      args
		id        uint64
		messageID uint64
		want1     [][]driver.Value
	}{
		{
			name: "",
			args: args{
				data:      []byte{2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 76, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 4, 0, 0, 0, 52, 0, 0, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0, 100, 0, 0, 0},
				colTypes:  []uint8{common.TSDB_DATA_TYPE_INT},
				rows:      13,
				precision: 0,
			},
			id:        2,
			messageID: 1,
			want1: [][]driver.Value{
				{int32(100)}, {int32(100)}, {int32(100)}, {int32(100)}, {int32(100)}, {int32(100)}, {int32(100)}, {int32(100)}, {int32(100)}, {int32(100)}, {int32(100)}, {int32(100)}, {int32(100)},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, messageID, result := ParseTmqBlock(tt.args.data, tt.args.colTypes, tt.args.rows, tt.args.precision)
			if id != tt.id {
				t.Errorf("ParseTmqBlock() id = %v, want %v", id, tt.id)
			}
			if messageID != tt.messageID {
				t.Errorf("ParseTmqBlock() messageID = %v, want %v", messageID, tt.messageID)
			}
			if !reflect.DeepEqual(result, tt.want1) {
				t.Errorf("ParseBlock() result = %v, want %v", result, tt.want1)
			}
		})
	}
}

package parseblock

import (
	"database/sql/driver"
	"reflect"
	"testing"

	"github.com/taosdata/driver-go/v2/common"
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

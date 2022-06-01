package log

import (
	"fmt"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestTaosSqlLogFormatter_Format(t1 *testing.T) {
	type args struct {
		entry *logrus.Entry
	}
	now := time.Now()
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "normal",
			args: args{
				entry: &logrus.Entry{
					Time:    now,
					Message: "test message",
				},
			},
			want: []byte(fmt.Sprintf("%s %s %s\n", now.Format("01/02 15:04:05.000000"), ServerID, "test message")),
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &TaosSqlLogFormatter{}
			got, err := t.Format(tt.args.entry)
			assert.NoError(t1, err)
			assert.Equalf(t1, tt.want, got, "Format(%v)", tt.args.entry)
		})
	}
}

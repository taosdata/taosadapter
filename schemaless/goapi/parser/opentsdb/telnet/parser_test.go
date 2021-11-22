package telnet

import (
	"reflect"
	"testing"
	"time"
)

func TestUnmarshal(t *testing.T) {
	type args struct {
		data string
	}
	tests := []struct {
		name    string
		args    args
		want    *Point
		wantErr bool
	}{
		{
			name: "test",
			args: args{
				data: "put sys.if.bytes.out 1479496100 1.3E3 host=web01 interface=eth0",
			},
			want: &Point{
				Ts:     time.Unix(1479496100, 0),
				Metric: "sys.if.bytes.out",
				Value:  1.3e3,
				Tags: Tags([]*Tag{
					{
						Key:   "host",
						Value: "web01",
					}, {
						Key:   "interface",
						Value: "eth0",
					},
				}),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Unmarshal(tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("Unmarshal() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Unmarshal() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Unmarshal("put sys.if.bytes.out 1479496100 1.3E3 host=web01 interface=eth0")
	}
}

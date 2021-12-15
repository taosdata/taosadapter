package telnet

import (
	"reflect"
	"testing"
	"time"
)

// @author: xftan
// @date: 2021/12/14 15:13
// @description: test telnet unmarshal
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
		}, {
			name: "no tag",
			args: args{
				data: "put sys.if.bytes.out 1479496100 1.3E3 ",
			},
			want:    nil,
			wantErr: true,
		}, {
			name: "nothing",
			args: args{
				data: "",
			},
			want:    nil,
			wantErr: true,
		}, {
			name: "no metric",
			args: args{
				data: "put  ",
			},
			want:    nil,
			wantErr: true,
		}, {
			name: "14 ts",
			args: args{
				data: "put sys.if.bytes.out 1479496100.001 1.3E3 host=web01 interface=eth0",
			},
			want: &Point{
				Ts:     time.Unix(1479496100, 1e6),
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
		}, {
			name: "wrong 14 ts 1",
			args: args{
				data: "put sys.if.bytes.out 1479496100.00a 1.3E3 host=web01 interface=eth0",
			},
			want:    nil,
			wantErr: true,
		}, {
			name: "wrong 14 ts 2",
			args: args{
				data: "put sys.if.bytes.out 147949610a.001 1.3E3 host=web01 interface=eth0",
			},
			want:    nil,
			wantErr: true,
		}, {
			name: "wrong 14 ts 3",
			args: args{
				data: "put sys.if.bytes.out 147949610.0001 1.3E3 host=web01 interface=eth0",
			},
			want:    nil,
			wantErr: true,
		}, {
			name: "13 ts",
			args: args{
				data: "put sys.if.bytes.out 1479496100001 1.3E3 host=web01 interface=eth0",
			},
			want: &Point{
				Ts:     time.Unix(1479496100, 1e6),
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
		}, {
			name: "wrong 13 ts",
			args: args{
				data: "put sys.if.bytes.out 147949610000a 1.3E3 host=web01 interface=eth0",
			},
			want:    nil,
			wantErr: true,
		}, {
			name: "wrong ts",
			args: args{
				data: "put sys.if.bytes.out 147949610a 1.3E3 host=web01 interface=eth0",
			},
			want:    nil,
			wantErr: true,
		}, {
			name: "wrong ts2",
			args: args{
				data: "put sys.if.bytes.out 1479496100000000 1.3E3 host=web01 interface=eth0",
			},
			want:    nil,
			wantErr: true,
		}, {
			name: "no value",
			args: args{
				data: "put sys.if.bytes.out 1479496100",
			},
			want:    nil,
			wantErr: true,
		}, {
			name: "wrong value",
			args: args{
				data: "put sys.if.bytes.out 1479496100 13a host=web01 interface=eth0",
			},
			want:    nil,
			wantErr: true,
		}, {
			name: "wrong tag",
			args: args{
				data: "put sys.if.bytes.out 1479496100 13a hostweb01",
			},
			want:    nil,
			wantErr: true,
		}, {
			name: "wrong tag 2",
			args: args{
				data: "put sys.if.bytes.out 1479496100 13a =hostweb01",
			},
			want:    nil,
			wantErr: true,
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

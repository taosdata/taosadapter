package connpool

import (
	"testing"
	"unsafe"

	"github.com/taosdata/driver-go/v2/wrapper"
)

func TestPool_Get(t *testing.T) {
	type fields struct {
		maxConnect int
		maxIdle    int
		user       string
		password   string
	}
	tests := []struct {
		name    string
		fields  fields
		notNull bool
		wantErr bool
	}{
		{
			name: "test",
			fields: fields{
				maxConnect: 100,
				maxIdle:    0,
				user:       "root",
				password:   "taosdata",
			},
			notNull: true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, _ := NewConnPool(tt.fields.maxConnect, tt.fields.maxIdle, tt.fields.user, tt.fields.password)
			got, err := p.Get()
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.notNull && got.Value == nil {
				t.Errorf("Get() expect not null")
				return
			}
			wrapper.TaosClose(got.Value.(unsafe.Pointer))
		})
	}
}

func BenchmarkGet(b *testing.B) {
	p, _ := NewConnPool(1000, 0, "root", "taosdata")
	for i := 0; i < b.N; i++ {
		n, err := p.Get()
		if err != nil {
			b.Error(err)
			return
		}
		err = p.Put(n)
		if err != nil {
			b.Error(err)
			return
		}
	}
}

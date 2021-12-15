package rest

import (
	"fmt"
	"testing"
)

// @author: xftan
// @date: 2021/12/14 15:09
// @description: test encode des
func TestEncodeDes(t *testing.T) {
	type args struct {
		user     string
		password string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "test",
			args: args{
				user:     "root",
				password: "taosdata",
			},
			want:    "/KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04",
			wantErr: false,
		}, {
			name: "wrong length",
			args: args{
				user:     "aaaaaaaaaaaaaaaaaaaaaaaaa",
				password: "",
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EncodeDes(tt.args.user, tt.args.password)
			if (err != nil) != tt.wantErr {
				t.Errorf("EncodeDes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("EncodeDes() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// @author: xftan
// @date: 2021/12/14 15:10
// @description: test decode des
func TestDecodeDes(t *testing.T) {
	type args struct {
		auth string
	}
	tests := []struct {
		name         string
		args         args
		wantUser     string
		wantPassword string
		wantErr      bool
	}{
		{
			name: "test",
			args: args{
				auth: "/KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04",
			},
			wantUser:     "root",
			wantPassword: "taosdata",
			wantErr:      false,
		}, {
			name: "wrong",
			args: args{
				auth: "/",
			},
			wantUser:     "",
			wantPassword: "",
			wantErr:      true,
		}, {
			name: "wrong length",
			args: args{
				auth: "YQ==",
			},
			wantUser:     "",
			wantPassword: "",
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotUser, gotPassword, err := DecodeDes(tt.args.auth)
			if (err != nil) != tt.wantErr {
				t.Errorf("DecodeDes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotUser != tt.wantUser {
				fmt.Println(len(gotUser))
				t.Errorf("DecodeDes() gotUser = %v, want %v", gotUser, tt.wantUser)
			}
			if gotPassword != tt.wantPassword {
				t.Errorf("DecodeDes() gotPassword = %v, want %v", gotPassword, tt.wantPassword)
			}
		})
	}
}

func BenchmarkEncodeDes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		s, err := EncodeDes("root", "taosdata")
		if err != nil {
			b.Error(err)
			return
		}
		_ = s
	}
}

func BenchmarkDecodeDes(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _, err := DecodeDes("/KfeAzX/f9na8qdtNZmtONryp201ma04bEl8LcvLUd7a8qdtNZmtONryp201ma04")
		if err != nil {
			b.Error(err)
			return
		}
	}
}

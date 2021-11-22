package tools

import "testing"

func BenchmarkDecodeBasic(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _, err := DecodeBasic("cm9vdDp0YW9zZGF0YQ==")
		if err != nil {
			b.Error(err)
			return
		}
	}
}

func TestDecodeBasic(t *testing.T) {
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
			name: "root",
			args: args{
				auth: "cm9vdDp0YW9zZGF0YQ==",
			},
			wantUser:     "root",
			wantPassword: "taosdata",
			wantErr:      false,
		}, {
			name: "wrong",
			args: args{
				auth: "wrong base64",
			},
			wantUser:     "",
			wantPassword: "",
			wantErr:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotUser, gotPassword, err := DecodeBasic(tt.args.auth)
			if (err != nil) != tt.wantErr {
				t.Errorf("DecodeBasic() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotUser != tt.wantUser {
				t.Errorf("DecodeBasic() gotUser = %v, want %v", gotUser, tt.wantUser)
			}
			if gotPassword != tt.wantPassword {
				t.Errorf("DecodeBasic() gotPassword = %v, want %v", gotPassword, tt.wantPassword)
			}
		})
	}
}

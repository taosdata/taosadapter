package sqltype

import "testing"

func TestGetSqlType(t *testing.T) {
	tests := []struct {
		sql      string
		expected SqlType
	}{
		{"insert into table", InsertType},     // Starts with "insert"
		{"INSERT into table", InsertType},     // Uppercase "INSERT"
		{"  insert into table", InsertType},   // "insert" with leading whitespace
		{"select * from table", SelectType},   // Starts with "select"
		{"SELECT * from table", SelectType},   // Uppercase "SELECT"
		{"  select * from table", SelectType}, // "select" with leading whitespace
		{"delete from table", OtherType},      // Does not match "insert" or "select"
		{"ins", OtherType},                    // Less than 6 characters
		{"selec", OtherType},                  // Less than 6 characters
		{"", OtherType},                       // Empty string
		{"   ", OtherType},                    // Whitespace-only string
		{"插入数据", OtherType},                   // Non-ASCII characters
	}

	for _, test := range tests {
		result := GetSqlType(test.sql)
		if result != test.expected {
			t.Errorf("GetSqlType(%q) = %d; want %d", test.sql, result, test.expected)
		}
	}
}

func TestRemoveSpacesAndLowercase(t *testing.T) {
	type args struct {
		str       string
		charCount int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "basic test",
			args: args{
				str:       "  Hello World  ",
				charCount: 0,
			},
			want: "helloworld",
		},
		{
			name: "with charCount less than string length",
			args: args{
				str:       "  Hello World  ",
				charCount: 5,
			},
			want: "hello",
		},
		{
			name: "with charCount more than string length",
			args: args{
				str:       "  Hello World  ",
				charCount: 20,
			},
			want: "helloworld",
		},
		{
			name: "empty string",
			args: args{
				str:       "",
				charCount: 0,
			},
			want: "",
		},
		{
			name: "string with only spaces",
			args: args{
				str:       "     ",
				charCount: 0,
			},
			want: "",
		},
		{
			name: "string with mixed spaces and characters",
			args: args{
				str:       " A B C D E ",
				charCount: 0,
			},
			want: "abcde",
		},
		{
			name: "string with mixed spaces and characters with charCount",
			args: args{
				str:       " A B C D E ",
				charCount: 3,
			},
			want: "abc",
		},
		{
			name: "string with no spaces",
			args: args{
				str:       "NoSpacesHere",
				charCount: 0,
			},
			want: "nospaceshere",
		},
		{
			name: "string with no spaces and charCount",
			args: args{
				str:       "NoSpacesHere",
				charCount: 5,
			},
			want: "nospa",
		},
		{
			name: "string with special characters",
			args: args{
				str:       "  Hello, World!  ",
				charCount: 0,
			},
			want: "hello,world!",
		},
		{
			name: "string with special characters and charCount",
			args: args{
				str:       "  Hello, World!  ",
				charCount: 7,
			},
			want: "hello,w",
		},
		{
			name: "string with newline and tab characters",
			args: args{
				str:       "\nHello\tWorld\n",
				charCount: 0,
			},
			want: "helloworld",
		},
		{
			name: "string with newline and tab characters and charCount",
			args: args{
				str:       "\nHello\tWorld\n",
				charCount: 5,
			},
			want: "hello",
		},
		{
			name: "string with unicode spaces",
			args: args{
				str:       " Hello World ", // Note: these are non-breaking spaces (U+00A0)
				charCount: 0,
			},
			want: "helloworld",
		},
		{
			name: "string with unicode spaces and charCount",
			args: args{
				str:       " Hello World ", // Note: these are non-breaking spaces (U+00A0)
				charCount: 5,
			},
			want: "hello",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RemoveSpacesAndLowercase(tt.args.str, tt.args.charCount); got != tt.want {
				t.Errorf("RemoveSpacesAndLowercase() = %v, want %v", got, tt.want)
			}
		})
	}
}

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
	}

	for _, test := range tests {
		result := GetSqlType(test.sql)
		if result != test.expected {
			t.Errorf("GetSqlType(%q) = %d; want %d", test.sql, result, test.expected)
		}
	}
}

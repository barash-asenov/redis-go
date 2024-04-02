package payload

import (
	"reflect"
	"testing"
)

func TestGenerateBulkString(t *testing.T) {
	type args struct {
		payload []byte
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			name: "when generating echo command reply",
			args: args{payload: []byte("hello")},
			want: []byte("$5\r\nhello\r\n"),
		},
		{
			name: "when request contains multiple words",
			args: args{payload: []byte("hello world from another side")},
			want: []byte("$29\r\nhello world from another side\r\n"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GenerateBulkString(tt.args.payload); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GenerateBulkString() = %q, want %q", string(got), string(tt.want))
			}
		})
	}
}

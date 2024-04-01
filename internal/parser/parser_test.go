package parser_test

import (
	"reflect"
	"testing"

	"github.com/codecrafters-io/redis-starter-go/internal/parser"
)

func TestParseRequest(t *testing.T) {
	type args struct {
		content []byte
	}
	tests := []struct {
		name    string
		args    args
		want    *parser.RedisRequest
		wantErr bool
	}{
		{
			name: "llen command",
			args: args{
				content: []byte("*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n"),
			},
			want: &parser.RedisRequest{
				Command: "LLEN",
				Payload: []string{"mylist"},
			},
			wantErr: false,
		},
		{
			name: "returns uppercase command",
			args: args{
				content: []byte("*2\r\n$4\r\nllen\r\n$6\r\nmylist\r\n"),
			},
			want: &parser.RedisRequest{
				Command: "LLEN",
				Payload: []string{"mylist"},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parser.ParseRequest(tt.args.content)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseRequest() = %v, want %v", got, tt.want)
			}
		})
	}
}

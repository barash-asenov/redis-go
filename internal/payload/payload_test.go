package payload

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestGenerateSimpleErrorString(t *testing.T) {
	testCases := map[string]struct {
		args           []interface{}
		expectedResult string
	}{
		"when XRANGE result given with 2 elements as nested interface": {
			args: []interface{}{
				[]interface{}{
					"1526985054069-0",
					[]interface{}{
						"temperature",
						"36",
						"humidity",
						"95",
					},
				},
				[]interface{}{
					"1526985054079-0",
					[]interface{}{
						"temperature",
						"37",
						"humidity",
						"94",
					},
				},
			},
			expectedResult: "*2\r\n*2\r\n$15\r\n1526985054069-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n36\r\n$8\r\nhumidity\r\n$2\r\n95\r\n*2\r\n$15\r\n1526985054079-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n37\r\n$8\r\nhumidity\r\n$2\r\n94\r\n",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			res, err := GenerateNestedListToString(tc.args)

			require.NoError(t, err)
			assert.Equal(t, tc.expectedResult, res)
		})
	}
}

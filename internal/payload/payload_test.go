package payload

import (
	"reflect"
	"testing"

	"github.com/codecrafters-io/redis-starter-go/internal/structures/stream"
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

func TestGenerateNestedListToString(t *testing.T) {
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
		"when data is coming from stream": {
			args: []interface{}{
				func() interface{} {
					data := stream.Data{
						ID: "1526985054069-0",
						Values: []string{
							"temperature",
							"36",
							"humidity",
							"95",
						},
					}

					return data.ToInterface()
				}(),
				func() interface{} {
					data := stream.Data{
						ID: "1526985054079-0",
						Values: []string{
							"temperature",
							"37",
							"humidity",
							"94",
						},
					}

					return data.ToInterface()
				}(),
			},
			expectedResult: "*2\r\n*2\r\n$15\r\n1526985054069-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n36\r\n$8\r\nhumidity\r\n$2\r\n95\r\n*2\r\n$15\r\n1526985054079-0\r\n*4\r\n$11\r\ntemperature\r\n$2\r\n37\r\n$8\r\nhumidity\r\n$2\r\n94\r\n",
		},
		"when XREAD result given": {
			args: []interface{}{
				[]interface{}{
					"stream_key",
					[]interface{}{
						[]interface{}{
							"0-1",
							[]interface{}{
								"temperature",
								"95",
							},
						},
					},
				},
				[]interface{}{
					"other_stream_key",
					[]interface{}{
						[]interface{}{
							"0-2",
							[]interface{}{
								"humidity",
								"97",
							},
						},
					},
				},
			},
			expectedResult: "*2\r\n*2\r\n$10\r\nstream_key\r\n*1\r\n*2\r\n$3\r\n0-1\r\n*2\r\n$11\r\ntemperature\r\n$2\r\n95\r\n*2\r\n$16\r\nother_stream_key\r\n*1\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$8\r\nhumidity\r\n$2\r\n97\r\n",
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

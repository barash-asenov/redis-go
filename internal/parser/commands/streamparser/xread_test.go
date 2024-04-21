package streamparser

import (
	"reflect"
	"testing"
)

func TestParseXReadCommand(t *testing.T) {
	type args struct {
		payloads []string
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		want1   []string
		wantErr bool
	}{
		{
			name: "when single value given",
			args: args{
				payloads: []string{"streams", "stream-key", "0-0"},
			},
			want:    []string{"stream-key"},
			want1:   []string{"0-0"},
			wantErr: false,
		},
		{
			name: "when 2 value given",
			args: args{
				payloads: []string{"streams", "stream_key1", "stream_key2", "0-0", "0-1"},
			},
			want:    []string{"stream_key1", "stream_key2"},
			want1:   []string{"0-0", "0-1"},
			wantErr: false,
		},
		{
			name: "when many value given",
			args: args{
				payloads: []string{"streams", "stream_key1", "stream_key2", "stream_3", "stream_4", "0-0", "0-1", "0-2", "0-3"},
			},
			want:    []string{"stream_key1", "stream_key2", "stream_3", "stream_4"},
			want1:   []string{"0-0", "0-1", "0-2", "0-3"},
			wantErr: false,
		},
		{
			name: "when value are provided not even",
			args: args{
				payloads: []string{"streams", "stream_key1", "stream_key2", "0-0", "0-1", "0-2"},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := ParseXReadCommand(tt.args.payloads)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseXReadCommand() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseXReadCommand() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("ParseXReadCommand() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

package streamfn_test

import (
	"testing"

	"github.com/codecrafters-io/redis-starter-go/internal/streamfn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIncrementID(t *testing.T) {
	testCases := map[string]struct {
		id            string
		expectedError bool
		expectedID    string
	}{
		"when valid stream ID given": {
			id:            "0-0",
			expectedError: false,
			expectedID:    "0-1",
		},
		"when invalid stream ID given": {
			id:            "11-",
			expectedError: true,
		},
		"when invalid sequence given": {
			id:            "11-55a",
			expectedError: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			res, err := streamfn.IncrementID(tc.id)

			if tc.expectedError {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.expectedID, res)
		})
	}
}

package stream_test

import (
	"fmt"
	"testing"

	"github.com/codecrafters-io/redis-starter-go/internal/structures/stream"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsert(t *testing.T) {
	testCases := map[string]struct {
		key           string
		value         map[string]string
		trie          *stream.NumericTrie
		expectedError error
		expectedTrie  *stream.NumericTrie
	}{
		"when trie is nil": {
			key:           "1-1",
			trie:          nil,
			expectedError: fmt.Errorf("Invalid trie"),
		},
		"when invalid input given": {
			key: "11",
			trie: &stream.NumericTrie{
				Root: &stream.Node{},
			},
			expectedError: fmt.Errorf("Invalid format for the key. Please give {int64}-{int64} format"),
		},
		"when input already exists": {
			key: "0-1",
			trie: &stream.NumericTrie{
				Root: &stream.Node{
					Children: [10]*stream.Node{
						{
							BiggestSequence: 1,
							Data:            map[int64]map[string]string{},
						},
					},
				},
			},
			expectedError: fmt.Errorf("The ID specified in XADD is equal or smaller than the target stream top item"),
		},
		"when bigger value already exists": {
			key: "0-2",
			trie: &stream.NumericTrie{
				Root: &stream.Node{
					Children: [10]*stream.Node{
						nil,
						{
							BiggestSequence: 1, // 1-1
							Data:            map[int64]map[string]string{},
						},
					},
				},
				Depth: 1,
			},
			expectedError: fmt.Errorf("The ID specified in XADD is equal or smaller than the target stream top item"),
		},
		"when trying to add 0-0 to empty trie": {
			key: "0-0",
			trie: &stream.NumericTrie{
				Root: &stream.Node{},
			},
			expectedError: fmt.Errorf("The ID specified in XADD must be greater than 0-0"),
		},
		"when bigger sequence already exists": {
			key: "100-1",
			value: map[string]string{
				"key-1": "value-1",
			},
			trie: &stream.NumericTrie{
				Root: &stream.Node{
					Children: [10]*stream.Node{
						nil,
						{
							Children: [10]*stream.Node{
								{
									Children: [10]*stream.Node{
										{
											Data: map[int64]map[string]string{
												5: {
													"key-5": "value-5",
												},
											},
											BiggestSequence: 5, // 100
										},
									},
								},
							},
						},
					},
				},
				Depth: 3,
			},
			expectedError: fmt.Errorf("The ID specified in XADD is equal or smaller than the target stream top item"),
		},
		"when adding value to empty trie": {
			key: "100-5151",
			value: map[string]string{
				"key-10": "value-100",
			},
			trie: &stream.NumericTrie{
				Root: &stream.Node{},
			},
			expectedTrie: &stream.NumericTrie{
				Root: &stream.Node{
					Children: [10]*stream.Node{
						nil,
						{
							Children: [10]*stream.Node{
								{
									Children: [10]*stream.Node{
										{
											Data: map[int64]map[string]string{
												5151: {
													"key-10": "value-100",
												},
											},
											BiggestSequence: 5151, // 100
										},
									},
								},
							},
						},
					},
				},
				Depth: 3,
			},
		},
		"when 99-0 exists and tries to add 100-0": {
			key: "100-0",
			value: map[string]string{
				"key-1": "value-1",
			},
			trie: &stream.NumericTrie{
				Root: &stream.Node{
					Children: [10]*stream.Node{
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						{
							Children: [10]*stream.Node{
								nil,
								nil,
								nil,
								nil,
								nil,
								nil,
								nil,
								nil,
								{
									Data: map[int64]map[string]string{},
								},
							},
						},
					},
				},
				Depth: 2,
			},
			expectedTrie: &stream.NumericTrie{
				Root: &stream.Node{
					Children: [10]*stream.Node{
						nil,
						{
							Children: [10]*stream.Node{
								{
									Children: [10]*stream.Node{
										{
											Data: map[int64]map[string]string{
												0: {
													"key-1": "value-1",
												},
											},
											BiggestSequence: 0, // 100
										},
									},
								},
							},
						},
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						{
							Children: [10]*stream.Node{
								nil,
								nil,
								nil,
								nil,
								nil,
								nil,
								nil,
								nil,
								{
									Data: map[int64]map[string]string{},
								},
							},
						},
					},
				},
				Depth: 3,
			},
		},
		"when appending to an existing sequence node": {
			key: "100-5",
			value: map[string]string{
				"key-5": "value-5",
			},
			trie: &stream.NumericTrie{
				Root: &stream.Node{
					Children: [10]*stream.Node{
						nil,
						{
							Children: [10]*stream.Node{
								{
									Children: [10]*stream.Node{
										{
											Data: map[int64]map[string]string{
												0: {
													"key-1": "value-1",
												},
											},
											BiggestSequence: 0, // 100
										},
									},
								},
							},
						},
					},
				},
				Depth: 3,
			},
			expectedTrie: &stream.NumericTrie{
				Root: &stream.Node{
					Children: [10]*stream.Node{
						nil,
						{
							Children: [10]*stream.Node{
								{
									Children: [10]*stream.Node{
										{
											Data: map[int64]map[string]string{
												0: {
													"key-1": "value-1",
												},
												5: {
													"key-5": "value-5",
												},
											},
											BiggestSequence: 5, // 100
										},
									},
								},
							},
						},
					},
				},
				Depth: 3,
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			trie := tc.trie

			err := trie.Insert(tc.key, tc.value)

			if tc.expectedError != nil {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.expectedError.Error())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.expectedTrie, trie)
		})
	}
}
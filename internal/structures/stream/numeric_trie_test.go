package stream_test

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/structures/stream"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsert(t *testing.T) {
	testCases := map[string]struct {
		key           string
		value         stream.Data
		trie          *stream.NumericTrie
		expectedId    string
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
			expectedError: fmt.Errorf("Invalid format for the key. Please give {int64}-{int64/*} format"),
		},
		"when input already exists": {
			key: "0-1",
			trie: &stream.NumericTrie{
				Root: &stream.Node{
					Children: [10]*stream.Node{
						{
							BiggestSequence: 1,
							Data:            map[int64]stream.Data{},
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
							Data:            map[int64]stream.Data{},
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
			value: stream.Data{
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
											Data: map[int64]stream.Data{
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
			value: stream.Data{
				"key-10": "value-100",
			},
			trie: &stream.NumericTrie{
				Root: &stream.Node{},
			},
			expectedId: "100-5151",
			expectedTrie: &stream.NumericTrie{
				Root: &stream.Node{
					Children: [10]*stream.Node{
						nil,
						{
							Children: [10]*stream.Node{
								{
									Children: [10]*stream.Node{
										{
											Data: map[int64]stream.Data{
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
			value: stream.Data{
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
									Data: map[int64]stream.Data{},
								},
							},
						},
					},
				},
				Depth: 2,
			},
			expectedId: "100-0",
			expectedTrie: &stream.NumericTrie{
				Root: &stream.Node{
					Children: [10]*stream.Node{
						nil,
						{
							Children: [10]*stream.Node{
								{
									Children: [10]*stream.Node{
										{
											Data: map[int64]stream.Data{
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
									Data: map[int64]stream.Data{},
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
			value: stream.Data{
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
											Data: map[int64]stream.Data{
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
			expectedId: "100-5",
			expectedTrie: &stream.NumericTrie{
				Root: &stream.Node{
					Children: [10]*stream.Node{
						nil,
						{
							Children: [10]*stream.Node{
								{
									Children: [10]*stream.Node{
										{
											Data: map[int64]stream.Data{
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
		"when auto incrementing non existing timestamp": {
			key: "100-*",
			value: stream.Data{
				"key-5": "value-5",
			},
			trie: &stream.NumericTrie{
				Root:  &stream.Node{},
				Depth: 0,
			},
			expectedId: "100-0",
			expectedTrie: &stream.NumericTrie{
				Root: &stream.Node{
					Children: [10]*stream.Node{
						nil,
						{
							Children: [10]*stream.Node{
								{
									Children: [10]*stream.Node{
										{
											Data: map[int64]stream.Data{
												0: {
													"key-5": "value-5",
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
		},
		"when auto incrementing existing timestamp": {
			key: "100-*",
			value: stream.Data{
				"key-5": "value-5",
			},
			expectedId: "100-6",
			trie: &stream.NumericTrie{
				Root: &stream.Node{
					Children: [10]*stream.Node{
						nil,
						{
							Children: [10]*stream.Node{
								{
									Children: [10]*stream.Node{
										{
											Data: map[int64]stream.Data{
												0: {
													"key-1": "value-1",
												},
												5: {
													"key-1": "value-1",
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
			expectedTrie: &stream.NumericTrie{
				Root: &stream.Node{
					Children: [10]*stream.Node{
						nil,
						{
							Children: [10]*stream.Node{
								{
									Children: [10]*stream.Node{
										{
											Data: map[int64]stream.Data{
												0: {
													"key-1": "value-1",
												},
												5: {
													"key-1": "value-1",
												},
												6: {
													"key-5": "value-5",
												},
											},
											BiggestSequence: 6, // 100
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
		"when key is given 0-* on empty": {
			key: "0-*",
			value: stream.Data{
				"key-5": "value-5",
			},
			expectedId: "0-1",
			trie:       &stream.NumericTrie{Root: &stream.Node{}},
			expectedTrie: &stream.NumericTrie{
				Root: &stream.Node{
					Children: [10]*stream.Node{
						{
							Data: map[int64]stream.Data{
								1: {
									"key-5": "value-5",
								},
							},
							BiggestSequence: 1, // 0
						},
					},
				},
				Depth: 1,
			},
		},
		"when id is given as total wildcard": {
			key: "*",
			value: stream.Data{
				"key-1": "value-1",
			},
			expectedId: "100-0",
			trie: stream.NewNumericTrie(func() time.Time {
				return time.Date(1970, 1, 1, 0, 0, 0, int(100*time.Millisecond.Nanoseconds()), time.UTC)
			}),
			expectedTrie: &stream.NumericTrie{
				Root: &stream.Node{
					Children: [10]*stream.Node{
						nil,
						{
							Children: [10]*stream.Node{
								{
									Children: [10]*stream.Node{
										{
											Data: map[int64]stream.Data{
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
						nil,
					},
				},
				Depth: 3,
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			trie := tc.trie

			id, err := trie.Insert(tc.key, tc.value)

			if tc.expectedError != nil {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.expectedError.Error())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.expectedId, id)
			assert.EqualExportedValues(t, tc.expectedTrie, trie)
		})
	}
}

func TestRange(t *testing.T) {
	testCases := map[string]struct {
		tree         *stream.NumericTrie
		begin        string
		end          string
		expectedData []stream.Data
	}{
		"when full valid data given": {
			tree: &stream.NumericTrie{
				Root: &stream.Node{
					Children: [10]*stream.Node{
						nil,
						nil,
						{
							Children: [10]*stream.Node{
								{
									Data: map[int64]stream.Data{ // 20-0
										0: {
											"key": "20-0",
										},
									},
									BiggestSequence: 0,
								},
								{
									Data: map[int64]stream.Data{ // 21-1
										1: {
											"key": "21-1",
										},
									},
									BiggestSequence: 1,
								},
							},
						},
						{
							Children: [10]*stream.Node{
								{
									Children: [10]*stream.Node{
										{
											Children: [10]*stream.Node{
												{
													Children: [10]*stream.Node{
														{
															Data: map[int64]stream.Data{ // 30000-5
																5: {
																	"key": "30000-5",
																},
															},
															Children:        [10]*stream.Node{},
															BiggestSequence: 5,
														},
													},
												},
											},
										},
									},
								},
								nil,
								{
									Data: map[int64]stream.Data{ // 32-8
										8: {
											"key": "32-8",
										},
									},
									Children:        [10]*stream.Node{},
									BiggestSequence: 8,
								},
							},
						},
					},
				},
			},
			begin: "21-0",
			end:   "39-9",
			expectedData: []stream.Data{
				{
					"key": "21-1",
				},
				{
					"key": "32-8",
				},
			},
		},
		"only big value is searched": {
			tree: &stream.NumericTrie{
				Root: &stream.Node{
					Children: [10]*stream.Node{
						nil,
						nil,
						{
							Children: [10]*stream.Node{
								{
									Data: map[int64]stream.Data{ // 20-0
										0: {
											"key": "20-0",
										},
									},
									BiggestSequence: 0,
								},
								{
									Data: map[int64]stream.Data{ // 21-1
										1: {
											"key": "21-1",
										},
									},
									BiggestSequence: 1,
								},
							},
						},
						{
							Children: [10]*stream.Node{
								{
									Children: [10]*stream.Node{
										{
											Children: [10]*stream.Node{
												{
													Children: [10]*stream.Node{
														{
															Data: map[int64]stream.Data{ // 30000-5
																5: {
																	"key": "30000-5",
																},
															},
															Children:        [10]*stream.Node{},
															BiggestSequence: 5,
														},
													},
												},
											},
										},
									},
								},
								nil,
								{
									Data: map[int64]stream.Data{ // 32-8
										8: {
											"key": "32-8",
										},
									},
									Children:        [10]*stream.Node{},
									BiggestSequence: 8,
								},
							},
						},
					},
				},
			},
			begin: "100-0",
			end:   "300000-9",
			expectedData: []stream.Data{
				{
					"key": "30000-5",
				},
			},
		},
		"only small value is searched": {
			tree: &stream.NumericTrie{
				Root: &stream.Node{
					Children: [10]*stream.Node{
						nil,
						nil,
						{
							Children: [10]*stream.Node{
								{
									Data: map[int64]stream.Data{ // 20-0
										0: {
											"key": "20-0",
										},
									},
									BiggestSequence: 0,
								},
								{
									Data: map[int64]stream.Data{ // 21-1
										1: {
											"key": "21-1",
										},
									},
									BiggestSequence: 1,
								},
							},
						},
						{
							Children: [10]*stream.Node{
								{
									Children: [10]*stream.Node{
										{
											Children: [10]*stream.Node{
												{
													Children: [10]*stream.Node{
														{
															Data: map[int64]stream.Data{ // 30000-5
																5: {
																	"key": "30000-5",
																},
															},
															Children:        [10]*stream.Node{},
															BiggestSequence: 5,
														},
													},
												},
											},
										},
									},
								},
								nil,
								{
									Data: map[int64]stream.Data{ // 32-8
										8: {
											"key": "32-8",
										},
									},
									Children:        [10]*stream.Node{},
									BiggestSequence: 8,
								},
							},
						},
					},
				},
			},
			begin: "20-0",
			end:   "20-9",
			expectedData: []stream.Data{
				{
					"key": "20-0",
				},
			},
		},
		"can limit the search with sequence": {
			tree: &stream.NumericTrie{
				Root: &stream.Node{
					Children: [10]*stream.Node{
						nil,
						nil,
						{
							Children: [10]*stream.Node{
								{
									Data: map[int64]stream.Data{ // 20-0
										0: {
											"key": "20-0",
										},
										1: {
											"key": "20-1",
										},
										2: {
											"key": "20-2",
										},
										3: {
											"key": "20-3",
										},
										4: {
											"key": "20-4",
										},
									},
									BiggestSequence: 0,
								},
								{
									Data: map[int64]stream.Data{ // 21-1
										1: {
											"key": "21-1",
										},
									},
									BiggestSequence: 1,
								},
							},
						},
						{
							Children: [10]*stream.Node{
								{
									Children: [10]*stream.Node{
										{
											Children: [10]*stream.Node{
												{
													Children: [10]*stream.Node{
														{
															Data: map[int64]stream.Data{ // 30000-5
																5: {
																	"key": "30000-5",
																},
															},
															Children:        [10]*stream.Node{},
															BiggestSequence: 5,
														},
													},
												},
											},
										},
									},
								},
								nil,
								{
									Data: map[int64]stream.Data{ // 32-8
										8: {
											"key": "32-8",
										},
									},
									Children:        [10]*stream.Node{},
									BiggestSequence: 8,
								},
							},
						},
					},
				},
			},
			begin: "20-0",
			end:   "20-3",
			expectedData: []stream.Data{
				{
					"key": "20-0",
				},
				{
					"key": "20-1",
				},
				{
					"key": "20-2",
				},
				{
					"key": "20-3",
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			res, err := tc.tree.Range(tc.begin, tc.end)

			require.NoError(t, err)

			// order res
			sort.Slice(res, func(i, j int) bool {
				return res[i]["key"] < res[j]["key"]
			})

			assert.Equal(t, tc.expectedData, res)
		})
	}
}

package stream

import (
	"fmt"
	"strconv"
	"strings"
)

const DigitCount = 10

type Node struct {
	Children        [DigitCount]*Node
	Data            map[int64]map[string]string // Only last nodes contain data, this also means this is a terminate node if this value is not null
	BiggestSequence int64                       // We can maintain this value in order to avoid looping through the data every single time
}

type NumericTrie struct {
	Root  *Node
	Depth int64
}

// Key 0-0 is not accepted
// Key should always be incremental
func (t *NumericTrie) Insert(key string, value map[string]string) error {
	if t == nil || t.Root == nil {
		return fmt.Errorf("Invalid trie")
	}

	timestampMilliDigits, sequence, err := ValidateAndParseKey(key)
	if err != nil {
		return fmt.Errorf("Unable to parse key: %w", err)
	}

	currentNode := t.Root

	for i, timestampDigit := range timestampMilliDigits {
		maxDigit := 0

		for i, child := range currentNode.Children {
			if child != nil {
				maxDigit = i
			}
		}

		if int(timestampDigit) < maxDigit && int(t.Depth) <= i+1 {
			return fmt.Errorf("The ID specified in XADD is equal or smaller than the target stream top item")
		}

		if currentNode.Children[timestampDigit] != nil {
			if i == len(timestampMilliDigits)-1 {
				if sequence <= currentNode.Children[timestampDigit].BiggestSequence {
					return fmt.Errorf("The ID specified in XADD is equal or smaller than the target stream top item")
				}

				currentNode.Children[timestampDigit].Data[sequence] = value
				currentNode.Children[timestampDigit].BiggestSequence = sequence
			}

			currentNode = currentNode.Children[timestampDigit]
			continue
		}

		newNode := &Node{}

		if i == len(timestampMilliDigits)-1 {
			// Data exists only when it's a terminate node
			newNode.Data = map[int64]map[string]string{}

			// Biggest sequence
			newNode.Data[sequence] = value
			newNode.BiggestSequence = sequence
		}

		t.Depth = int64(i) + 1

		currentNode.Children[timestampDigit] = newNode
		currentNode = currentNode.Children[timestampDigit]
	}

	return nil
}

// Key should have the following format;
// {timestamp_millisecond}-{int64}
// {timestamp_millisecond} can also be represented as int64 number
func ValidateAndParseKey(key string) ([]uint8, int64, error) {
	keyParts := strings.Split(key, "-")

	if len(keyParts) != 2 {
		return nil, 0, fmt.Errorf("Invalid format for the key. Please give {int64}-{int64} format")
	}

	if keyParts[0] == "0" && keyParts[1] == "0" {
		return nil, 0, fmt.Errorf("The ID specified in XADD must be greater than 0-0")
	}

	// Validate the fisrt part
	_, err := strconv.ParseInt(keyParts[0], 10, 64)
	if err != nil {
		return nil, 0, fmt.Errorf("Invalid value for the first part of the key: %s", keyParts[0])
	}

	timestampDigits := make([]uint8, 0, len(keyParts[0]))

	for _, digit := range strings.Split(keyParts[0], "") {
		timestampDigit, err := strconv.ParseUint(string(digit), 10, 4)
		if err != nil {
			return nil, 0, fmt.Errorf("Insert has expected a digit, but received: %s. Error: %w\n", string(digit), err)
		}

		timestampDigits = append(timestampDigits, uint8(timestampDigit))
	}

	// Validate the second part
	sequence, err := strconv.ParseInt(keyParts[1], 10, 64)
	if err != nil {
		return nil, 0, fmt.Errorf("Invalid value for the second part of the key: %s", keyParts[1])
	}

	return timestampDigits, sequence, nil
}

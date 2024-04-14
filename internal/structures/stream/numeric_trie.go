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
func (t *NumericTrie) Insert(key string, value map[string]string) (string, error) {
	if t == nil || t.Root == nil {
		return "", fmt.Errorf("Invalid trie")
	}

	timestampMilliDigits, sequence, err := validateAndParseKey(key)
	if err != nil {
		return "", err
	}

	insertedId := ""

	currentNode := t.Root

	for i, char := range strings.Split(timestampMilliDigits, "") {
		timestampDigit, err := strconv.ParseUint(char, 10, 4)
		if err != nil {
			return "", fmt.Errorf("Invalid digit: %s, %w", char, err)
		}

		maxDigit := 0
		var sequenceNumber int64 = 0

		if sequence == "*" {
			if currentNode.Children[timestampDigit] != nil {
				sequenceNumber = currentNode.Children[timestampDigit].BiggestSequence + 1
			}

			if timestampMilliDigits == "0" {
				sequenceNumber = 1
			}
		} else {
			sequenceNumber, err = strconv.ParseInt(sequence, 10, 64)
			if err != nil {
				return "", fmt.Errorf("Failed to convert the sequence number to int: %s, %w", sequence, err)
			}
		}

		for i, child := range currentNode.Children {
			if child != nil {
				maxDigit = i
			}
		}

		if int(timestampDigit) < maxDigit && int(t.Depth) <= i+1 {
			return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}

		if currentNode.Children[timestampDigit] != nil {
			if i == len(timestampMilliDigits)-1 {

				if sequenceNumber <= currentNode.Children[timestampDigit].BiggestSequence {
					return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
				}

				currentNode.Children[timestampDigit].Data[sequenceNumber] = value
				currentNode.Children[timestampDigit].BiggestSequence = sequenceNumber
				insertedId = fmt.Sprintf("%s-%d", timestampMilliDigits, sequenceNumber)
			}

			currentNode = currentNode.Children[timestampDigit]
			continue
		}

		newNode := &Node{}

		if i == len(timestampMilliDigits)-1 {
			// Data exists only when it's a terminate node
			newNode.Data = map[int64]map[string]string{}

			// Biggest sequence
			newNode.Data[sequenceNumber] = value
			newNode.BiggestSequence = sequenceNumber
			insertedId = fmt.Sprintf("%s-%d", timestampMilliDigits, sequenceNumber)
		}

		t.Depth = int64(i) + 1

		currentNode.Children[timestampDigit] = newNode
		currentNode = currentNode.Children[timestampDigit]
	}

	return insertedId, nil
}

func uint8SliceToString(slice []uint8) string {
	fmt.Printf("slice: %s\n", slice)
	fmt.Printf("slice: %+v\n", slice)
	str := make([]byte, len(slice))
	for i, v := range slice {
		fmt.Println("i:", v)
		str[i] = byte(v)
	}
	fmt.Printf("str is: %+v -- %+v", string(str), str)
	return string(str)
}

// Key should have the following format;
// {timestamp_millisecond}-{int64}
// {timestamp_millisecond} can also be represented as int64 number / but can be given as *
// which will trigger auto assignment
func validateAndParseKey(key string) (string, string, error) {
	keyParts := strings.Split(key, "-")

	if len(keyParts) != 2 {
		return "", "", fmt.Errorf("Invalid format for the key. Please give {int64}-{int64/*} format")
	}

	if keyParts[0] == "0" && keyParts[1] == "0" {
		return "", "", fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	}

	// Validate the fisrt part
	_, err := strconv.ParseInt(keyParts[0], 10, 64)
	if err != nil {
		return "", "", fmt.Errorf("Invalid value for the first part of the key: %s", keyParts[0])
	}

	return keyParts[0], keyParts[1], nil
}

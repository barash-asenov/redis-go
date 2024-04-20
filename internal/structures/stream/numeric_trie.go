package stream

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const DigitCount = 10

var ErrNotFound = errors.New("Not found")

type Data struct {
	ID     string
	Values []string // Key Value Pairs
}

func DataFromMap(ID string, values map[string]string) *Data {
	data := &Data{}

	data.ID = ID

	for key, val := range values {
		data.Values = append(data.Values, key, val)
	}

	return data
}

func (d *Data) AsMap() map[string]string {
	res := make(map[string]string)

	for i := 0; i < len(d.Values); i++ {
		if i%2 == 0 {
			continue
		}

		res[d.Values[i-1]] = d.Values[i]
	}

	return res
}

func (d *Data) ToInterface() interface{} {
	structuedInterface := make([]interface{}, 0, 2)

	structuedInterface = append(structuedInterface, interface{}(d.ID))

	values := make([]interface{}, 0, len(d.Values)*2)

	for _, val := range d.Values {
		values = append(values, interface{}(val))
	}

	structuedInterface = append(structuedInterface, values)

	return structuedInterface
}

type Node struct {
	Children        [DigitCount]*Node
	Data            map[int64]*Data // Only last nodes contain data, this also means this is a terminate node if this value is not null
	BiggestSequence int64           // We can maintain this value in order to avoid looping through the data every single time
}

type NumericTrie struct {
	Root  *Node
	Depth int64

	nowFn func() time.Time
}

func NewNumericTrie(nowFn func() time.Time) *NumericTrie {
	return &NumericTrie{
		Root: &Node{},

		nowFn: nowFn,
	}
}

// Key 0-0 is not accepted
// Key should always be incremental
func (t *NumericTrie) Insert(key string, values map[string]string) (string, error) {
	if t == nil || t.Root == nil {
		return "", fmt.Errorf("Invalid trie")
	}

	if key == "*" {
		key = fmt.Sprintf("%d-%s", t.nowFn().UnixMilli(), "*")
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

				insertedId = fmt.Sprintf("%s-%d", timestampMilliDigits, sequenceNumber)
				currentNode.Children[timestampDigit].Data[sequenceNumber] = DataFromMap(insertedId, values)
				currentNode.Children[timestampDigit].BiggestSequence = sequenceNumber
			}

			currentNode = currentNode.Children[timestampDigit]
			continue
		}

		newNode := &Node{}

		if i == len(timestampMilliDigits)-1 {
			// Biggest sequence
			insertedId = fmt.Sprintf("%s-%d", timestampMilliDigits, sequenceNumber)
			newNode.Data = make(map[int64]*Data)
			newNode.Data[sequenceNumber] = DataFromMap(insertedId, values)
			newNode.BiggestSequence = sequenceNumber
		}

		t.Depth = int64(i) + 1

		currentNode.Children[timestampDigit] = newNode
		currentNode = currentNode.Children[timestampDigit]
	}

	return insertedId, nil
}

// Beging and End provided
// Validate that begin cannot be smaller than end, but they can be same
// They can only include timestamp values, they don't need to include sequence part
func (t *NumericTrie) Range(begin string, end string) ([]*Data, error) {
	if t == nil || t.Root == nil {
		return nil, fmt.Errorf("Invalid trie")
	}

	beginTimestamp := begin[0:strings.Index(begin, "-")]
	endTimestamp := end[0:strings.Index(end, "-")]
	beginSequence := begin[strings.Index(begin, "-")+1:]
	endSequence := end[strings.Index(end, "-")+1:]

	beginTimestampInt, err := strconv.Atoi(beginTimestamp)
	if err != nil {
		return nil, fmt.Errorf("Failed to convert beginTimestamp to int: %w", err)
	}
	endTimestampInt, err := strconv.Atoi(endTimestamp)
	if err != nil {
		return nil, fmt.Errorf("Failed to convert endTimestamp to int: %w", err)
	}
	beginSequenceInt, err := strconv.Atoi(beginSequence)
	if err != nil {
		return nil, fmt.Errorf("Failed to convert beginSequence to int: %w", err)
	}
	endSequenceInt, err := strconv.Atoi(endSequence)
	if err != nil {
		return nil, fmt.Errorf("Failed to convert endSequence to int: %w", err)
	}

	if beginTimestampInt > endTimestampInt {
		return nil, fmt.Errorf("Invalid range given, begin cannot be bigger than end")
	}

	foundNodes, err := t.findNestedNodes(beginTimestampInt, endTimestampInt, beginSequenceInt, endSequenceInt, nil, "")
	if err != nil {
		return nil, err
	}

	return foundNodes, nil
}

func (t *NumericTrie) findNestedNodes(beginTimestamp, endTimestamp, beginSequence, endSequence int, currentNode *Node, currentVal string) ([]*Data, error) {
	if currentNode == nil {
		currentNode = t.Root
	}

	foundData := make([]*Data, 0)

	for ind, subNode := range currentNode.Children {
		if subNode == nil {
			continue
		}

		newValue := fmt.Sprintf("%s%d", currentVal, ind)
		newValueInt, err := strconv.Atoi(newValue)
		if err != nil {
			return nil, err
		}

		if newValueInt > endTimestamp {
			break
		}

		subNodes, err := t.findNestedNodes(beginTimestamp, endTimestamp, beginSequence, endSequence, subNode, newValue)
		if err != nil {
			return nil, err
		}

		foundData = append(foundData, subNodes...)

		if subNode.Data != nil && newValueInt <= endTimestamp && newValueInt >= beginTimestamp {
			for key, val := range subNode.Data {
				if newValueInt == beginTimestamp && key < int64(beginSequence) {
					continue
				}

				if newValueInt == endTimestamp && key > int64(endSequence) {
					continue
				}

				foundData = append(foundData, val)
			}
		}
	}

	return foundData, nil
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

package payload

import (
	"fmt"
	"strconv"
)

func GenerateBulkString(payload []byte) []byte {
	bulkString := make([]byte, 0, 256)

	bulkString = append(bulkString, '$')
	bulkString = append(bulkString, []byte(strconv.Itoa(len(payload)))...)
	bulkString = append(bulkString, []byte{'\r', '\n'}...)
	bulkString = append(bulkString, payload...)
	bulkString = append(bulkString, []byte{'\r', '\n'}...)

	return bulkString
}

func GenerateBasicString(payload []byte) []byte {
	basicString := make([]byte, 0, 256)

	basicString = append(basicString, '+')
	basicString = append(basicString, payload...)
	basicString = append(basicString, []byte{'\r', '\n'}...)

	return basicString
}

func GenerateNullString() []byte {
	return []byte("$-1\r\n")
}

func GenerateSimpleErrorString(payload []byte) []byte {
	return []byte(fmt.Sprintf("-%s\r\n", string(payload)))
}

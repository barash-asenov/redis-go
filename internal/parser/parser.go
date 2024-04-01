package parser

import (
	"fmt"
	"strconv"
	"strings"
)

type RedisRequest struct {
	Command string
	Payload []string
}

func ParseRequest(content []byte) (*RedisRequest, error) {
	redisRequest := &RedisRequest{}

	reader := strings.NewReader(string(content))

	firstByte, err := reader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("Invalid request: %w", err)
	}

	if firstByte != '*' {
		return nil, fmt.Errorf("Request needs to be start with *, but given %q", rune(firstByte))
	}

	numberOfParams, err := readUntilCRLFAsInt(reader)
	if err != nil {
		return nil, fmt.Errorf("readUntilCRLFAsInt: %w", err)
	}

	for i := 0; i < numberOfParams; i++ {
		char, err := reader.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("Failed to read byte: %w", err)
		}

		if char != '$' {
			return nil, fmt.Errorf("Invalid redis request, expected $, received: %q", char)
		}

		contentLen, err := readUntilCRLFAsInt(reader)
		if err != nil {
			return nil, fmt.Errorf("readUntilCRLFAsInt: %w", err)
		}

		buf := make([]byte, contentLen)
		n, err := reader.Read(buf)
		if err != nil {
			return nil, fmt.Errorf("Failed during read: %w", err)
		}

		content := buf[:n]

		if redisRequest.Command == "" {
			redisRequest.Command = strings.ToUpper(string(content))

			err := discardCRLF(reader)
			if err != nil {
				return nil, fmt.Errorf("Failed to discardCRLF: %w", err)
			}

			continue
		}

		redisRequest.Payload = append(redisRequest.Payload, string(content))
		err = discardCRLF(reader)
		if err != nil {
			return nil, fmt.Errorf("Failed to discardCRLF: %w", err)
		}
	}

	return redisRequest, nil
}

func readUntilCRLFAsInt(reader *strings.Reader) (int, error) {
	content, err := readUntilCRLF(reader)
	if err != nil {
		return 0, fmt.Errorf("readUntilCRLF failed: %w", err)
	}

	contentAsInt, err := strconv.Atoi(string(content))
	if err != nil {
		return 0, fmt.Errorf("Failed to convert the content to Int: %w", err)
	}

	return contentAsInt, nil
}

func readUntilCRLF(reader *strings.Reader) ([]byte, error) {
	content := make([]byte, 0, 256)

	for {
		char, err := reader.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("Failed to read byte: %w", err)
		}

		if char == '\r' {
			break
		}

		content = append(content, char)
	}

	// read the '\n' character
	_, err := reader.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("Failed to read byte: %w", err)
	}

	return content, nil
}

func discardCRLF(reader *strings.Reader) error {
	discarded := make([]byte, 2)

	n, err := reader.Read(discarded)
	if err != nil {
		return fmt.Errorf("Failed to read to discarded data: %w", err)
	}

	if string(discarded[:n]) != "\r\n" {
		return fmt.Errorf("Expected a CRLF return, but received: %s", string(discarded[:n]))
	}

	return nil
}

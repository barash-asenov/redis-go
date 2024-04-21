package streamfn

import (
	"fmt"
	"strconv"
	"strings"
)

func IncrementID(id string) (string, error) {
	timestamp, sequence, found := strings.Cut(id, "-")

	if !found {
		return "", fmt.Errorf("Invalid ID Format: %s", id)
	}

	sequenceInt, err := strconv.Atoi(sequence)
	if err != nil {
		return "", fmt.Errorf("Invalid sequence number: %w", err)
	}

	incerementedSequence := sequenceInt + 1

	return fmt.Sprintf("%s-%d", timestamp, incerementedSequence), nil
}

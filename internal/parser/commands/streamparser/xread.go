package streamparser

import "fmt"

func ParseXReadCommand(payloads []string) ([]string, []string, error) {
	keys := []string{}
	ids := []string{}

	if len(payloads) < 3 {
		return nil, nil, fmt.Errorf("Invalid payload given")
	}

	if len(payloads)%2 == 0 {
		return nil, nil, fmt.Errorf("Payload order is wrong")
	}

	for i := 1; i <= len(payloads)/2; i++ {
		keys = append(keys, payloads[i])
		ids = append(ids, payloads[len(payloads)/2+i])
	}

	return keys, ids, nil
}

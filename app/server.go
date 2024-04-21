package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/commands"
	"github.com/codecrafters-io/redis-starter-go/internal/parser"
	"github.com/codecrafters-io/redis-starter-go/internal/payload"
	"github.com/codecrafters-io/redis-starter-go/internal/store"
)

var (
	kvStore     = store.NewKVStore()
	streamStore = store.NewStream()
	typeCommand = commands.NewTypeCommand(kvStore, streamStore)
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	connCh := make(chan net.Conn)
	errCh := make(chan error)

	// starting 10 workers...
	startWorkers(10, connCh, errCh)
	go logger(errCh)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		// send the connection to a worker
		connCh <- conn
	}
}

func startWorkers(quantitiy int, connCh <-chan net.Conn, errCh chan<- error) {
	for i := 1; i <= quantitiy; i++ {
		go worker(i, connCh, errCh)
	}
}

func logger(errCh <-chan error) {
	for err := range errCh {
		log.Println("Error happened:", err.Error())
	}
}

// worker will work until the connection is closed
func worker(id int, connCh <-chan net.Conn, errCh chan<- error) {
	for conn := range connCh {
		err := handleConnection(id, conn)
		if err != nil {
			errCh <- fmt.Errorf("Failed to handle connection: %w", err)
		}
	}
}

func handleConnection(connID int, conn net.Conn) error {
	defer conn.Close()

	for {
		content, err := readFromConnection(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Println("Breaking due to EOF..., ID:", connID)
				break
			}

			return fmt.Errorf("Failed to read from connection %d: %w", connID, err)
		}

		parsed, err := parser.ParseRequest(content)
		if err != nil {
			return fmt.Errorf("Failed to parse redis request: %w", err)
		}

		var writeContent []byte

		if parsed.Command == "ECHO" && len(parsed.Payload) != 0 {
			writeContent = payload.GenerateBulkString([]byte(parsed.Payload[0]))
		} else if parsed.Command == "SET" && len(parsed.Payload) > 1 {
			expirationMs := 0
			if len(parsed.Payload) > 3 {
				if strings.EqualFold(parsed.Payload[2], "PX") {
					expirationMs, err = strconv.Atoi(parsed.Payload[3])
					if err != nil {
						return fmt.Errorf("Failed to convert expiration payload to int: %w", err)
					}
				}
			}

			kvStore.Set(parsed.Payload[0], parsed.Payload[1], int64(expirationMs))

			writeContent = payload.GenerateBasicString([]byte("OK"))
		} else if parsed.Command == "GET" && len(parsed.Payload) != 0 {
			val, found := kvStore.Get(parsed.Payload[0])

			if !found {
				writeContent = payload.GenerateNullString()
			} else {
				writeContent = payload.GenerateBulkString([]byte(val))
			}
		} else if parsed.Command == "XADD" && len(parsed.Payload) > 2 {
			kvPairs := []string{}

			key := parsed.Payload[0]
			id := parsed.Payload[1]

			for i := 3; i < len(parsed.Payload); i += 2 {
				kvPairs = append(kvPairs, parsed.Payload[i-1], parsed.Payload[i])
			}

			res, err := streamStore.XAdd(key, id, kvPairs)
			if err != nil {
				writeContent = payload.GenerateSimpleErrorString([]byte(err.Error()))
			} else {
				writeContent = payload.GenerateBasicString([]byte(res))
			}

		} else if parsed.Command == "XRANGE" && len(parsed.Payload) > 2 {
			key := parsed.Payload[0]
			begin := parsed.Payload[1]
			end := parsed.Payload[2]

			res, err := streamStore.XRead(key, begin, end)
			if err != nil {
				return fmt.Errorf("Failed during XRead: %w", err)
			}

			writeContent = []byte(res)

		} else if parsed.Command == "TYPE" && len(parsed.Payload) != 0 {
			writeContent = typeCommand.GetType(parsed.Payload[0])
		} else {
			writeContent = payload.GenerateBasicString([]byte("PONG"))
		}

		_, err = conn.Write(writeContent)
		if err != nil {
			return fmt.Errorf("Failed to write to connection %d: %w", connID, err)
		}
	}

	return nil
}

func readFromConnection(conn net.Conn) ([]byte, error) {
	buf := make([]byte, 4096)

	n, err := conn.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("Failed to read from the connection: %w", err)
	}

	return buf[:n], nil
}

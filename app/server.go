package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/internal/parser"
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

		writeContent := []byte("+")

		if parsed.Command == "ECHO" && len(parsed.Payload) != 0 {
			writeContent = append(writeContent, []byte(parsed.Payload[0])...)
			writeContent = append(writeContent, []byte("\r\n")...)
		} else {
			writeContent = append(writeContent, []byte("PONG\r\n")...)
		}

		fmt.Println("write content:", string(writeContent))

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

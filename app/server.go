package main

import (
	"fmt"
	"log"
	"net"
	"os"
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

	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	log.Println("Will receive")

	for {
		content, err := readFromConnection(conn)
		if err != nil {
			log.Fatal("Failed to read from connection:", err)
		}

		log.Println("Received:", string(content))

		writeContent := []byte("+PONG\r\n")

		_, err = conn.Write(writeContent)
		if err != nil {
			log.Fatal("Failed to write to connection:", err)
		}
	}
}

func readFromConnection(conn net.Conn) ([]byte, error) {
	buf := make([]byte, 4096)

	n, err := conn.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("Failed to read from the connection: %w", err)
	}

	return buf[:n], nil
}

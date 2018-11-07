package tcp

import "net"

// ConnPublisher implements the publisher pattern for giving
// tcp connections.
type ConnPublisher struct {
	conn net.Conn
}

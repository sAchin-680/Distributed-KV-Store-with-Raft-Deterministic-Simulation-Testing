package transport

import "net"

// listenLoopback reserves a loopback port so a test can learn an address before
// the transport that will use it exists. Two nodes need each other's addresses
// up front, and binding to port 0 inside NewGRPC would not reveal them in time.
func listenLoopback() (net.Listener, error) {
	return net.Listen("tcp", "127.0.0.1:0")
}

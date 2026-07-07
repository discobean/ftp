package ftp

import (
	"net"
	"time"
)

// idleConn wraps a net.Conn to enforce an IDLE deadline: every Read and Write
// pushes the connection deadline forward by idle before the operation runs, so a
// peer that stalls (no bytes for idle) makes the next Read/Write fail with an
// i/o timeout instead of blocking forever. A transfer that keeps making progress
// resets the deadline on every chunk and is never cut short.
//
// It is installed on both the control and data connections when the
// DialWithIdleTimeout option is set. Refreshing per operation (rather than one
// absolute deadline) is what makes it safe on the control connection: the library
// only reads the control connection BEFORE and AFTER a data transfer, never
// during it, so a stale mid-transfer deadline is always replaced by the bump on
// the next actual read.
type idleConn struct {
	net.Conn
	idle time.Duration
}

// wrapIdleConn wraps conn with an idle deadline when idle > 0; otherwise it
// returns conn unchanged (so behaviour is identical to upstream when unset).
func wrapIdleConn(conn net.Conn, idle time.Duration) net.Conn {
	if conn == nil || idle <= 0 {
		return conn
	}
	if _, already := conn.(*idleConn); already {
		return conn
	}
	return &idleConn{Conn: conn, idle: idle}
}

func (c *idleConn) bump() {
	if c.idle > 0 {
		_ = c.Conn.SetDeadline(time.Now().Add(c.idle))
	}
}

func (c *idleConn) Read(b []byte) (int, error) {
	c.bump()
	return c.Conn.Read(b)
}

func (c *idleConn) Write(b []byte) (int, error) {
	c.bump()
	return c.Conn.Write(b)
}

// Handshake forwards to the wrapped connection when it supports one (a
// *tls.Conn), preserving StorFrom's explicit zero-byte-file TLS handshake. The
// idle deadline is applied first so a stalled handshake is bounded too.
func (c *idleConn) Handshake() error {
	if h, ok := c.Conn.(interface{ Handshake() error }); ok {
		c.bump()
		return h.Handshake()
	}
	return nil
}

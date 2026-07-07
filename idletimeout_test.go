package ftp

import (
	"net"
	"testing"
	"time"
)

// TestIdleConnReadTimesOut proves the core guarantee: a stalled peer (accepts
// the connection but never sends anything) makes a Read on an idleConn fail with
// an i/o timeout within the idle window, instead of blocking forever.
func TestIdleConnReadTimesOut(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		time.Sleep(2 * time.Second) // hold the conn open but never write
		_ = c.Close()
	}()

	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()

	ic := &idleConn{Conn: raw, idle: 100 * time.Millisecond}
	start := time.Now()
	_, err = ic.Read(make([]byte, 16))
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected a timeout error, got nil")
	}
	ne, ok := err.(net.Error)
	if !ok || !ne.Timeout() {
		t.Fatalf("expected a net timeout error, got %v", err)
	}
	if elapsed > time.Second {
		t.Fatalf("read did not time out promptly: %v", elapsed)
	}
}

// TestIdleConnBumpExtendsOnProgress proves a progressing transfer is not cut
// short: repeated reads that each return data keep succeeding well past the idle
// window because every read refreshes the deadline.
func TestIdleConnBumpExtendsOnProgress(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	go func() {
		// write one byte every 50ms for ~300ms — longer than the 100ms idle,
		// but no single gap exceeds it
		for i := 0; i < 6; i++ {
			_, _ = server.Write([]byte{byte(i)})
			time.Sleep(50 * time.Millisecond)
		}
	}()

	ic := &idleConn{Conn: client, idle: 100 * time.Millisecond}
	buf := make([]byte, 1)
	for i := 0; i < 6; i++ {
		if _, err := ic.Read(buf); err != nil {
			t.Fatalf("read %d failed though progress was being made: %v", i, err)
		}
	}
}

func TestWrapIdleConn(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()

	// idle <= 0 returns the conn unchanged
	if got := wrapIdleConn(a, 0); got != a {
		t.Fatal("idle 0 should return the conn unchanged")
	}
	// positive idle wraps
	wrapped := wrapIdleConn(a, time.Second)
	if _, ok := wrapped.(*idleConn); !ok {
		t.Fatalf("expected *idleConn, got %T", wrapped)
	}
	// never double-wraps
	if again := wrapIdleConn(wrapped, time.Second); again != wrapped {
		t.Fatal("should not double-wrap an idleConn")
	}
	// nil is passed through
	if got := wrapIdleConn(nil, time.Second); got != nil {
		t.Fatal("nil should pass through")
	}
}

type fakeHandshaker struct {
	net.Conn
	handshaked bool
}

func (f *fakeHandshaker) Handshake() error {
	f.handshaked = true
	return nil
}

func TestIdleConnHandshakeForwards(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()

	// forwards to a handshaker (e.g. *tls.Conn) — StorFrom's zero-byte path
	fh := &fakeHandshaker{Conn: a}
	ic := &idleConn{Conn: fh, idle: time.Second}
	if err := ic.Handshake(); err != nil {
		t.Fatalf("Handshake forward errored: %v", err)
	}
	if !fh.handshaked {
		t.Fatal("Handshake was not forwarded to the wrapped conn")
	}

	// a plain conn (no Handshake) is a harmless no-op
	ic2 := &idleConn{Conn: b, idle: time.Second}
	if err := ic2.Handshake(); err != nil {
		t.Fatalf("expected nil for a non-handshaker, got %v", err)
	}
}

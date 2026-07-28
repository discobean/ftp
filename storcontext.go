package ftp

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
)

// This file adds the two inert termination primitives from DocEvent's
// SEND-BUDGET-PLAN (stage 2): a context-aware Stor and a lock-free control
// connection force-closer. Neither changes the behaviour of the existing API —
// StorFrom delegates here with context.Background(), which can never cancel.

// StorWithContext is Stor with a cancellation contract for the DATA phase:
// when ctx is cancelled, the operation-local data connection is force-closed,
// which unblocks a transfer parked in a data-connection Write and makes the
// copy unwind. Cancellation ALWAYS yields a non-nil error, even when the
// server replied happily — closing the data connection is FTP's end-of-file
// signal, so the server may have stored a TRUNCATED file and said 226; that
// must never be reported as success.
//
// Scope (deliberate): only the data phase is governed by ctx. The control
// exchanges before and after the copy (PASV/EPSV, STOR, the closing status
// read) are bounded by DialWithIdleTimeout / DialWithShutTimeout as usual; a
// caller that must also interrupt a parked CONTROL operation uses ForceClose.
// The source reader is likewise the CALLER's to unblock: a copy parked in
// r.Read is not released by closing the data connection.
//
// After a CANCELLED call the control session must be treated as UNUSABLE and
// torn down (Close/Quit if the caller can, ForceClose if it cannot): servers
// may report an aborted transfer with MORE than one reply (e.g. 426 followed
// by 226), and only the first is consumed here — a subsequent command could
// read the stale leftover and desynchronise the session. (DocEvent's stage-3
// terminator force-closes the control connection on abort anyway.)
func (c *ServerConn) StorWithContext(ctx context.Context, path string, r io.Reader) error {
	return c.storFromContext(ctx, path, r, 0)
}

// StorFromWithContext is StorFrom with the same cancellation contract as
// StorWithContext.
func (c *ServerConn) StorFromWithContext(ctx context.Context, path string, r io.Reader, offset uint64) error {
	return c.storFromContext(ctx, path, r, offset)
}

// storFromContext is the single implementation behind Stor / StorFrom /
// StorWithContext / StorFromWithContext. With a never-done context it is
// byte-for-byte the historical StorFrom behaviour.
func (c *ServerConn) storFromContext(ctx context.Context, path string, r io.Reader, offset uint64) error {
	// Refuse to start a protocol exchange under an already-cancelled context:
	// returning before the STOR command leaves the control connection in a
	// clean, reusable state.
	if err := ctx.Err(); err != nil {
		return err
	}

	conn, err := c.cmdDataConnFrom(offset, "STOR %s", path)
	if err != nil {
		return err
	}

	// The closer must be idempotent: the cancellation AfterFunc and the normal
	// teardown below may both close, in any order, possibly concurrently. The
	// COPY still targets the ORIGINAL conn: routing it through a wrapper would
	// hide the connection's io.ReaderFrom (e.g. *net.TCPConn's sendfile/splice
	// fast path) from io.Copy and change legacy Stor dispatch, performance,
	// and error shape.
	closer := &dataConnCloser{conn: conn}
	stop := context.AfterFunc(ctx, func() { _ = closer.Close() })
	defer stop()

	var errs []error

	// if the upload fails we still need to try to read the server
	// response otherwise if the failure is not due to a connection problem,
	// for example the server denied the upload for quota limits, we miss
	// the response and we cannot use the connection to send other commands.
	if n, err := io.Copy(conn, r); err != nil {
		errs = append(errs, err)
	} else if n == 0 {
		// If we wrote no bytes and got no error, make sure we call
		// tls.Handshake on the connection as it won't get called
		// unless Write() is called. (See comment in openDataConn()).
		//
		// ProFTP doesn't like this and returns "Unable to build data
		// connection: Operation not permitted" when trying to upload
		// an empty file without this.
		if do, ok := conn.(interface{ Handshake() error }); ok {
			if err := do.Handshake(); err != nil {
				errs = append(errs, err)
			}
		}
	}

	// stop() is the LINEARIZATION POINT of the cancellation race. false means
	// the context was cancelled and the AfterFunc closer has run (or is
	// running — Close is once-guarded, so racing it is fine). It is evaluated
	// AFTER the copy, so a cancellation that fires between the last byte and
	// here is still treated as cancelled: the close may have cut buffered
	// bytes off in the kernel, and "copy returned nil" does not prove the
	// server received a complete file. Conversely, once stop() returns true
	// the transfer has won the race and a LATER cancellation cannot
	// retroactively fail it.
	cancelled := !stop()

	if err := closer.Close(); err != nil && !cancelled {
		// On the cancelled path a close error is not appended: whichever
		// goroutine won the Once (the AfterFunc closer or this call — both
		// may be racing here), there was exactly one underlying Close, its
		// result would double-report against ctx.Err() below, and the
		// cancellation is the authoritative outcome.
		errs = append(errs, err)
	}

	if cancelled {
		// The data connection was torn down MID-transfer, so the closing
		// status may legitimately be 426 (transfer aborted) — and a server
		// that saw the close as end-of-file may say 226. checkDataClose is the
		// bounded read that tolerates both (checkDataShut would wait on an
		// unbounded control read when no shut timeout is configured, and
		// treats 426 as an error).
		if err := c.checkDataClose(); err != nil {
			errs = append(errs, err)
		}
		errs = append(errs, ctx.Err())
	} else {
		if err := c.checkDataShut(); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// dataConnCloser coordinates the data connection's Close between the
// cancellation AfterFunc and the normal teardown: idempotent, safe to race,
// every caller gets the first Close's result. Deliberately NOT a net.Conn —
// the transfer copy must target the original conn so its io.ReaderFrom fast
// path stays visible to io.Copy (see storFromContext).
type dataConnCloser struct {
	conn net.Conn
	once sync.Once
	err  error
}

func (c *dataConnCloser) Close() error {
	c.once.Do(func() { c.err = c.conn.Close() })
	return c.err
}

// ForceClose immediately closes the control connection's underlying socket.
// No QUIT is sent, no ServerConn state is touched, and no lock is taken —
// unlike every other ServerConn method it is safe to call concurrently with
// an in-flight operation, and that is its purpose: unblocking a goroutine
// parked in a control-connection read or write when the normal teardown path
// cannot run (in DocEvent's send plugins the processor's Close waits on the
// command mutex, which the blocked operation itself holds).
//
// The close targets the RAWEST reachable socket: a TLS control connection is
// unwrapped (tls.Conn.NetConn) so no close-notify is attempted — a TLS-level
// Close can block for seconds on a wedged peer, which is exactly the
// situation ForceClose exists for. After ForceClose the session is unusable;
// the blocked operation unwinds with a connection error and the caller's
// normal cleanup applies.
//
// Safe concurrently because netConn is assigned once at Dial and never
// reassigned, and net.Conn.Close is documented safe alongside in-flight I/O.
func (c *ServerConn) ForceClose() error {
	return rawestConn(c.netConn).Close()
}

// rawestConn unwraps layered connections (idleConn, *tls.Conn — anything
// exposing NetConn) down to the innermost socket. Depth-bounded: a buggy or
// hostile wrapper (reachable via a custom dial function) whose NetConn
// returns itself or forms a cycle must degrade to closing an outer layer,
// never turn ForceClose into a spin. The bound is the ONLY cycle defense on
// purpose — an equality check on the interfaces would panic for an
// uncomparable dynamic conn type (e.g. a value type containing a slice).
// The library's own chain is at most idleConn → *tls.Conn → socket (depth 2).
func rawestConn(conn net.Conn) net.Conn {
	for depth := 0; depth < 8; depth++ {
		unwrapper, ok := conn.(interface{ NetConn() net.Conn })
		if !ok {
			return conn
		}
		inner := unwrapper.NetConn()
		if inner == nil {
			return conn
		}
		conn = inner
	}
	return conn
}

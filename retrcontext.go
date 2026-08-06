package ftp

import (
	"context"
	"errors"
	"io"
)

// RetrWithContext fetches path and copies its contents into w, returning the
// byte count, with a cancellation contract for the DATA phase: when ctx is
// cancelled the operation-local data connection is force-closed, which
// unblocks a copy parked in a data-connection Read against a stalled server
// and makes the transfer unwind. Cancellation ALWAYS yields a non-nil error,
// even when every byte had already arrived — the close races the server's
// closing status, so a cancelled call can never prove the transfer complete.
//
// The shape differs from Retr (which returns a streaming *Response) on
// purpose: a context-governed download must keep its cancellation watcher
// armed for the WHOLE read, which a returned ReadCloser cannot guarantee.
// This call is equivalent to Retr + io.Copy + Response.Close, so the final
// status is read exactly like Response.Close does (bounded wait; 426 after an
// early close tolerated unless DialWithStrictDataTransfers is set).
//
// Scope (deliberate, mirroring StorWithContext): only the data phase is
// governed by ctx. The control exchanges before and after the copy are
// bounded by DialWithIdleTimeout / DialWithShutTimeout as usual; a caller
// that must also interrupt a parked CONTROL operation uses ForceClose. The
// destination writer is likewise the CALLER's to unblock: a copy parked in
// w.Write is not released by closing the data connection.
//
// After a CANCELLED call the control session must be treated as UNUSABLE and
// torn down (Close/Quit if the caller can, ForceClose if it cannot): servers
// may report an aborted transfer with MORE than one reply (e.g. 426 followed
// by 226), and only the first is consumed here — a subsequent command could
// read the stale leftover and desynchronise the session.
func (c *ServerConn) RetrWithContext(ctx context.Context, path string, w io.Writer) (int64, error) {
	// Refuse to start a protocol exchange under an already-cancelled context:
	// returning before the RETR command leaves the control connection in a
	// clean, reusable state.
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	conn, err := c.cmdDataConnFrom(0, "RETR %s", path)
	if err != nil {
		return 0, err
	}

	// Idempotent closer shared by the cancellation AfterFunc and the normal
	// teardown — both may close, in any order, possibly concurrently. The copy
	// still READS the original conn (see storFromContext for why a wrapper
	// would change dispatch).
	closer := &dataConnCloser{conn: conn}
	stop := context.AfterFunc(ctx, func() { _ = closer.Close() })
	defer stop()

	var errs []error

	n, copyErr := io.Copy(w, conn)
	if copyErr != nil {
		errs = append(errs, copyErr)
	}

	// stop() is the LINEARIZATION POINT of the cancellation race (see
	// storFromContext): evaluated AFTER the copy, so a cancellation that fires
	// between the last byte and here still fails the call — the close may have
	// raced the server's closing status, and "copy hit EOF" does not prove the
	// server sent the whole file. Once stop() returns true, a later
	// cancellation cannot retroactively fail the transfer.
	cancelled := !stop()

	if err := closer.Close(); err != nil && !cancelled {
		errs = append(errs, err)
	}

	// Both branches read the closing status the way Response.Close does — a
	// BOUNDED wait tolerant of the local close racing the server's report
	// (checkDataShut would park forever on a server that says nothing when no
	// shut timeout is configured). Under DialWithStrictDataTransfers a 426 is
	// still an error, exactly as for Retr + Close.
	if err := c.checkDataClose(); err != nil {
		errs = append(errs, err)
	}
	if cancelled {
		errs = append(errs, ctx.Err())
	}

	return n, errors.Join(errs...)
}

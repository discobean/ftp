package ftp

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRetrWithContextNeverCancelled: with a never-done context the call is
// Retr + io.Copy + Response.Close — full content, nil error, reusable session.
func TestRetrWithContextNeverCancelled(t *testing.T) {
	mock, c := openConn(t, "127.0.0.1")

	// Seed the mock's file content (its RETR serves back what STOR captured).
	require.NoError(t, c.Stor("file", bytes.NewBufferString(testData)))

	var buf bytes.Buffer
	n, err := c.RetrWithContext(context.Background(), "file", &buf)
	require.NoError(t, err)
	assert.Equal(t, int64(len(testData)), n)
	assert.Equal(t, testData, buf.String())

	// The session stays clean: a normal command succeeds afterwards.
	require.NoError(t, c.NoOp())

	closeConn(t, mock, c, []string{"EPSV", "STOR", "EPSV", "RETR", "NOOP"})
}

// TestRetrWithContextPreCancelled: an already-cancelled context refuses to
// start — no RETR (or any data-conn setup) reaches the wire, and the control
// connection stays usable.
func TestRetrWithContextPreCancelled(t *testing.T) {
	mock, c := openConn(t, "127.0.0.1")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var buf bytes.Buffer
	n, err := c.RetrWithContext(ctx, "file", &buf)
	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, n)
	assert.NotContains(t, mock.commands, "RETR")
	assert.NotContains(t, mock.commands, "EPSV")

	require.NoError(t, c.NoOp())

	closeConn(t, mock, c, []string{"NOOP"})
}

// TestRetrWithContextCancelMidTransfer is the core contract: cancelling
// mid-download closes the operation-local data connection, which ends a copy
// fed by a server that would otherwise stream forever, and the call returns a
// ctx-attributable error — never success.
func TestRetrWithContextCancelMidTransfer(t *testing.T) {
	mock, err := newFtpMockExt(t, "127.0.0.1", "no-time", func(m *ftpMock) {
		m.retrMode = "drip"
	})
	require.NoError(t, err)
	defer mock.Close()

	c, err := Dial(mock.Addr(), DialWithShutTimeout(2*time.Second))
	require.NoError(t, err)
	require.NoError(t, c.Login("anonymous", "anonymous"))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		var buf bytes.Buffer
		// Endless source: this can only return because cancellation closed
		// the data connection out from under the read.
		_, err := c.RetrWithContext(ctx, "file", &buf)
		done <- err
	}()

	// Let some data flow (server dripping 32 KiB / 50 ms), then cancel.
	time.Sleep(300 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.Error(t, err, "a cancelled download must never report success")
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("cancellation did not end the endless download")
	}

	// A cancelled download makes the session UNUSABLE (the abort's closing
	// status may be more than one reply) — tear it down, never reuse it.
	require.NoError(t, c.ForceClose())
	mock.Wait()
}

// TestRetrWithContextCancelDoubleReply: some servers report an aborted
// transfer with TWO replies (426 then 226). Only the first is consumed, so
// the call still errors with the cancellation — and the leftover 226 is
// exactly why a cancelled session must be torn down, not reused.
func TestRetrWithContextCancelDoubleReply(t *testing.T) {
	mock, err := newFtpMockExt(t, "127.0.0.1", "no-time", func(m *ftpMock) {
		m.retrMode = "drip"
		m.dripDouble = true
	})
	require.NoError(t, err)
	defer mock.Close()

	c, err := Dial(mock.Addr(), DialWithShutTimeout(2*time.Second))
	require.NoError(t, err)
	require.NoError(t, c.Login("anonymous", "anonymous"))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		var buf bytes.Buffer
		_, err := c.RetrWithContext(ctx, "file", &buf)
		done <- err
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("cancellation did not end the download")
	}

	require.NoError(t, c.ForceClose())
	mock.Wait()
}

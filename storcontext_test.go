package ftp

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// zeroReader yields zero bytes forever — an unbounded source, so a transfer
// can only end because the DESTINATION side was torn down.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

// TestStorWithContextBackgroundParity: with a never-done context the new path
// behaves exactly like the historical Stor (which now delegates to it) — and
// once the transfer wins the stop() linearization point, a LATER cancellation
// cannot retroactively fail it.
func TestStorWithContextBackgroundParity(t *testing.T) {
	mock, c := openConn(t, "127.0.0.1")

	ctx, cancel := context.WithCancel(context.Background())
	content := "the quick brown fox"
	err := c.StorWithContext(ctx, "file", strings.NewReader(content))
	require.NoError(t, err)
	cancel() // after return: the completed transfer stays a success

	assert.Equal(t, content, mock.fileCont.String())

	closeConn(t, mock, c, []string{"EPSV", "STOR"})
}

// TestStorWithContextPreCancelled: an already-cancelled context refuses to
// start — no STOR (or any data-conn setup) reaches the wire, and the control
// connection stays usable.
func TestStorWithContextPreCancelled(t *testing.T) {
	mock, c := openConn(t, "127.0.0.1")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := c.StorWithContext(ctx, "file", strings.NewReader("payload"))
	require.ErrorIs(t, err, context.Canceled)
	assert.NotContains(t, mock.commands, "STOR")
	assert.NotContains(t, mock.commands, "EPSV")

	// The connection is still clean: a normal command succeeds afterwards.
	require.NoError(t, c.NoOp())

	closeConn(t, mock, c, []string{"NOOP"})
}

// TestStorWithContextCancelMidCopy is the core contract (SEND-BUDGET-PLAN
// D3/R4): cancelling mid-transfer closes the operation-local data connection,
// which unblocks a copy parked in a data-conn Write against a stalled server,
// and the call returns a ctx-attributable error — never success.
func TestStorWithContextCancelMidCopy(t *testing.T) {
	mock, err := newFtpMockExt(t, "127.0.0.1", "no-time", func(m *ftpMock) {
		m.storMode = "drip"
	})
	require.NoError(t, err)
	defer mock.Close()

	c, err := Dial(mock.Addr(), DialWithShutTimeout(2*time.Second))
	require.NoError(t, err)
	require.NoError(t, c.Login("anonymous", "anonymous"))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		// Unbounded source: this can only return because cancellation closed
		// the data connection out from under the parked Write.
		done <- c.StorWithContext(ctx, "file", zeroReader{})
	}()

	// Let the transfer park (buffers full, server dripping 32 KiB / 50 ms).
	time.Sleep(300 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.Error(t, err, "a cancelled transfer must never report success")
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("cancellation did not unblock the parked transfer")
	}

	// A cancelled upload makes the session UNUSABLE (the abort's closing
	// status may be more than one reply) — tear it down, never reuse it.
	require.NoError(t, c.ForceClose())
	mock.Wait()
}

// TestStorWithContextCancelDoubleReply: some servers report an aborted
// transfer with TWO replies (426 then 226). Only the first is consumed, so
// the call still errors with the cancellation — and the leftover 226 is
// exactly why a cancelled session must be torn down, not reused.
func TestStorWithContextCancelDoubleReply(t *testing.T) {
	mock, err := newFtpMockExt(t, "127.0.0.1", "no-time", func(m *ftpMock) {
		m.storMode = "drip"
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
		done <- c.StorWithContext(ctx, "file", zeroReader{})
	}()

	time.Sleep(300 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("cancellation did not unblock the parked transfer")
	}

	require.NoError(t, c.ForceClose())
	mock.Wait()
}

// TestStorWithContextCancelAtEOF pins the linearization point from the other
// side: the source cancels the context ON its final read, so the copy
// completes cleanly but cancellation precedes stop(). The call must report
// the cancellation — the data-conn close raced the tail bytes, so a clean
// copy return does not prove the server committed a complete file; SQS
// redelivery is chosen over trusting it.
func TestStorWithContextCancelAtEOF(t *testing.T) {
	mock, err := newFtpMock(t, "127.0.0.1")
	require.NoError(t, err)
	defer mock.Close()

	c, err := Dial(mock.Addr(), DialWithShutTimeout(2*time.Second))
	require.NoError(t, err)
	require.NoError(t, c.Login("anonymous", "anonymous"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src := &cancelAtEOFReader{data: strings.NewReader("payload"), cancel: cancel}

	err = c.StorWithContext(ctx, "file", src)
	require.Error(t, err, "cancellation before stop() must never report success")
	assert.ErrorIs(t, err, context.Canceled)

	require.NoError(t, c.ForceClose())
	mock.Wait()
}

// cancelAtEOFReader cancels the given context at the exact moment its data is
// exhausted — deterministically racing cancellation against a completing copy.
type cancelAtEOFReader struct {
	data   *strings.Reader
	cancel context.CancelFunc
}

func (r *cancelAtEOFReader) Read(p []byte) (int, error) {
	n, err := r.data.Read(p)
	if err == io.EOF {
		r.cancel()
	}
	return n, err
}

// TestStorDispatchPreservesReaderFrom guards the copy-dispatch parity of the
// legacy path: the transfer's io.Copy must target the ORIGINAL data conn so a
// connection's io.ReaderFrom fast path (e.g. *net.TCPConn sendfile/splice)
// stays selected — routing the copy through a wrapper would silently change
// dispatch, performance, and error shape for every existing Stor caller.
func TestStorDispatchPreservesReaderFrom(t *testing.T) {
	mock, err := newFtpMock(t, "127.0.0.1")
	require.NoError(t, err)
	defer mock.Close()

	var (
		mu        sync.Mutex
		recorders []*readFromRecorder
	)
	dialFunc := func(network, address string) (net.Conn, error) {
		conn, err := net.Dial(network, address)
		if err != nil {
			return nil, err
		}
		rec := &readFromRecorder{Conn: conn}
		mu.Lock()
		recorders = append(recorders, rec)
		mu.Unlock()
		return rec, nil
	}

	c, err := Dial(mock.Addr(), DialWithDialFunc(dialFunc))
	require.NoError(t, err)
	require.NoError(t, c.Login("anonymous", "anonymous"))

	content := "dispatch parity payload"
	// Strip the source's method set: strings.Reader implements io.WriterTo,
	// which io.Copy prefers over the destination's ReadFrom — a real S3 body
	// (like this bare reader) has no WriterTo, so dst dispatch decides.
	src := struct{ io.Reader }{strings.NewReader(content)}
	require.NoError(t, c.Stor("file", src))
	assert.Equal(t, content, mock.fileCont.String())

	mu.Lock()
	defer mu.Unlock()
	used := false
	for _, rec := range recorders {
		if rec.used.Load() {
			used = true
		}
	}
	assert.True(t, used, "io.Copy did not select the data conn's ReadFrom — the copy destination is wrapped")

	require.NoError(t, c.Quit())
	mock.Wait()
}

// readFromRecorder is a net.Conn that records whether io.Copy selected its
// ReadFrom fast path.
type readFromRecorder struct {
	net.Conn
	used atomic.Bool
}

func (c *readFromRecorder) ReadFrom(r io.Reader) (int64, error) {
	c.used.Store(true)
	return io.Copy(c.Conn, r)
}

// TestStorWithContextCancelSourceParked documents the deliberate boundary:
// a copy parked in the SOURCE's Read is not released by cancelling (only the
// data connection is ours to close) — the source's owner unblocks it, and the
// call then still reports the cancellation.
func TestStorWithContextCancelSourceParked(t *testing.T) {
	mock, err := newFtpMockExt(t, "127.0.0.1", "no-time", func(m *ftpMock) {
		m.storMode = "drip"
	})
	require.NoError(t, err)
	defer mock.Close()

	c, err := Dial(mock.Addr(), DialWithShutTimeout(2*time.Second))
	require.NoError(t, err)
	require.NoError(t, c.Login("anonymous", "anonymous"))

	release := make(chan struct{})
	src := &gatedSource{gate: release}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- c.StorWithContext(ctx, "file", src)
	}()

	time.Sleep(150 * time.Millisecond) // let the copy park in src.Read
	cancel()

	// Cancellation alone must NOT complete the call — the source is parked.
	select {
	case err := <-done:
		t.Fatalf("returned while the source was still parked: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	close(release) // the source owner aborts the source
	select {
	case err := <-done:
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("releasing the source did not unwind the transfer")
	}

	// Cancelled session: unusable by policy — tear down, never reuse.
	require.NoError(t, c.ForceClose())
	mock.Wait()
}

// gatedSource sends one byte, then parks every Read until gate closes, after
// which it returns an error (mimicking an aborted upstream source).
type gatedSource struct {
	gate chan struct{}
	sync.Mutex
	sent bool
}

func (s *gatedSource) Read(p []byte) (int, error) {
	s.Lock()
	first := !s.sent
	s.sent = true
	s.Unlock()
	if first && len(p) > 0 {
		p[0] = 'x'
		return 1, nil
	}
	<-s.gate
	return 0, errors.New("source aborted by owner")
}

// TestForceCloseUnblocksControlOp is the R4 F3 primitive proof: a goroutine
// blocked in a CONTROL operation — before any data connection exists — is
// unblocked by ForceClose from another goroutine. No lock is involved, so
// this works even when the caller's own serialization mutex is held by the
// blocked operation (the exact deadlock the primitive exists to avoid).
func TestForceCloseUnblocksControlOp(t *testing.T) {
	mock, err := newFtpMockExt(t, "127.0.0.1", "no-time", func(m *ftpMock) {
		m.dataSilent = true
	})
	require.NoError(t, err)
	defer mock.Close()

	c, err := Dial(mock.Addr())
	require.NoError(t, err)
	require.NoError(t, c.Login("anonymous", "anonymous"))

	done := make(chan error, 1)
	go func() {
		// Parks reading the EPSV reply that never comes — no data conn exists
		// yet, so only the control-conn closer can unblock this.
		done <- c.Stor("file", strings.NewReader("payload"))
	}()

	time.Sleep(150 * time.Millisecond) // let Stor park in the control read
	require.NoError(t, c.ForceClose())

	select {
	case err := <-done:
		require.Error(t, err, "a force-closed control op must not report success")
	case <-time.After(10 * time.Second):
		t.Fatal("ForceClose did not unblock the parked control operation")
	}

	// Idempotent enough: a second call must not panic (its error, if any, is
	// the platform's double-close report and is irrelevant).
	_ = c.ForceClose()
	mock.Wait()
}

// TestRawestConn: the unwrap helper reaches the innermost socket through both
// wrapper layers (idleConn and *tls.Conn), so ForceClose never performs a TLS
// close-notify (which can block on a wedged peer).
func TestRawestConn(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()

	tlsConn := tls.Client(a, &tls.Config{InsecureSkipVerify: true}) //nolint:gosec // fixture
	wrapped := wrapIdleConn(tlsConn, time.Second)

	raw := rawestConn(wrapped)
	require.Same(t, a, raw, "expected the raw pipe end under idleConn+tls.Conn")

	// Closing the rawest conn unblocks the peer without any TLS traffic.
	readDone := make(chan error, 1)
	go func() {
		_, err := b.Read(make([]byte, 1))
		readDone <- err
	}()
	require.NoError(t, raw.Close())
	select {
	case err := <-readDone:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("closing the raw conn did not unblock the peer")
	}

	// A plain conn unwraps to itself.
	c, d := net.Pipe()
	defer c.Close()
	defer d.Close()
	require.Same(t, c, rawestConn(c))

	// A cyclic/self-returning unwrapper (possible via a custom dial func)
	// must terminate at the depth bound, not spin ForceClose forever — and
	// must not panic even when the conn's dynamic type is UNCOMPARABLE (a
	// value type containing a slice makes interface equality panic, so the
	// bound must be the only cycle defense).
	cyc := cyclicConn{Conn: c, pad: make([]byte, 1)}
	require.NotNil(t, rawestConn(cyc))
}

// cyclicConn's NetConn returns itself — the pathological unwrap case. A
// VALUE type with a slice field: uncomparable on purpose, so any interface
// equality check in the unwrap loop would panic.
type cyclicConn struct {
	net.Conn
	pad []byte
}

func (c cyclicConn) NetConn() net.Conn { return c }

// TestDataConnCloser: every closer gets the FIRST Close's result; racing
// closes are safe (run with -race).
func TestDataConnCloser(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	oc := &dataConnCloser{conn: a}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, oc.Close())
		}()
	}
	wg.Wait()
}

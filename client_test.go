package ftp

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/textproto"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	testData = "Just some text"
	testDir  = "mydir"
)

func TestConnPASV(t *testing.T) {
	testConn(t, true)
}

func TestConnEPSV(t *testing.T) {
	testConn(t, false)
}

func testConn(t *testing.T, disableEPSV bool) {
	assert := assert.New(t)
	mock, c := openConn(t, "127.0.0.1", DialWithTimeout(5*time.Second), DialWithDisabledEPSV(disableEPSV))

	err := c.Login("anonymous", "anonymous")
	assert.NoError(err)

	err = c.NoOp()
	assert.NoError(err)

	err = c.ChangeDir("incoming")
	assert.NoError(err)

	dir, err := c.CurrentDir()
	if assert.NoError(err) {
		assert.Equal("/incoming", dir)
	}

	data := bytes.NewBufferString(testData)
	err = c.Stor("test", data)
	assert.NoError(err)

	_, err = c.List(".")
	assert.NoError(err)

	err = c.Rename("test", "tset")
	assert.NoError(err)

	// Read without deadline
	r, err := c.Retr("tset")
	if assert.NoError(err) {
		buf, err := io.ReadAll(r)
		if assert.NoError(err) {
			assert.Equal(testData, string(buf))
		}

		assert.NoError(r.Close(), "close reader")
		assert.NoError(r.Close(), "close reader twitce") // test we can close two times
	}

	// Read with deadline
	r, err = c.Retr("tset")
	if assert.NoError(err) {
		if err := r.SetDeadline(time.Now()); err != nil {
			t.Fatal(err)
		}
		_, err = io.ReadAll(r)
		assert.ErrorContains(err, "i/o timeout")
		assert.NoError(r.Close(), "close reader")
	}

	// Read with offset
	r, err = c.RetrFrom("tset", 5)
	if assert.NoError(err) {
		buf, err := io.ReadAll(r)
		if assert.NoError(err) {
			expected := testData[5:]
			assert.Equal(expected, string(buf))
		}

		assert.NoError(r.Close(), "close reader")
	}

	data2 := bytes.NewBufferString(testData)
	err = c.Append("tset", data2)
	assert.NoError(err)

	// Read without deadline, after append
	r, err = c.Retr("tset")
	if assert.NoError(err) {
		buf, err := io.ReadAll(r)
		if assert.NoError(err) {
			assert.Equal(testData+testData, string(buf))
		}

		assert.NoError(r.Close(), "close reader")
	}

	fileSize, err := c.FileSize("magic-file")
	assert.NoError(err)
	assert.Equal(int64(42), fileSize)

	_, err = c.FileSize("not-found")
	assert.Error(err)

	entry, err := c.GetEntry("magic-file")
	if err != nil {
		t.Error(err)
	}
	if entry == nil {
		t.Fatal("expected entry, got nil")
	}
	if entry.Size != 42 {
		t.Errorf("entry size %d, expected %d", entry.Size, 42)
	}
	if entry.Type != EntryTypeFile {
		t.Errorf("entry type %q, expected %q", entry.Type, EntryTypeFile)
	}
	if entry.Name != "magic-file" {
		t.Errorf("entry name %q, expected %q", entry.Name, "magic-file")
	}

	entry, err = c.GetEntry("multiline-dir")
	if err != nil {
		t.Error(err)
	}
	if entry == nil {
		t.Fatal("expected entry, got nil")
	}
	if entry.Size != 0 {
		t.Errorf("entry size %d, expected %d", entry.Size, 0)
	}
	if entry.Type != EntryTypeFolder {
		t.Errorf("entry type %q, expected %q", entry.Type, EntryTypeFolder)
	}
	if entry.Name != "multiline-dir" {
		t.Errorf("entry name %q, expected %q", entry.Name, "multiline-dir")
	}

	// A server padding the MLST entry line with several leading spaces
	// (upstream issue #338) must still parse.
	entry, err = c.GetEntry("leading-space-file")
	if err != nil {
		t.Error(err)
	}
	if entry == nil {
		t.Fatal("expected entry, got nil")
	}
	if entry.Size != 42 {
		t.Errorf("entry size %d, expected %d", entry.Size, 42)
	}
	if entry.Type != EntryTypeFile {
		t.Errorf("entry type %q, expected %q", entry.Type, EntryTypeFile)
	}
	if entry.Name != "leading-space-file" {
		t.Errorf("entry name %q, expected %q", entry.Name, "leading-space-file")
	}

	err = c.Delete("tset")
	assert.NoError(err)

	err = c.MakeDir(testDir)
	assert.NoError(err)

	err = c.ChangeDir(testDir)
	assert.NoError(err)

	err = c.ChangeDirToParent()
	assert.NoError(err)

	entries, err := c.NameList("/")
	assert.NoError(err)
	assert.Equal([]string{"/incoming"}, entries)

	err = c.RemoveDir(testDir)
	assert.NoError(err)

	err = c.Logout()
	assert.NoError(err)

	if err = c.Quit(); err != nil {
		t.Fatal(err)
	}

	// Wait for the connection to close
	mock.Wait()

	err = c.NoOp()
	assert.Error(err, "should error on closed conn")
}

// TestConnect tests the legacy Connect function
func TestConnect(t *testing.T) {
	mock, err := newFtpMock(t, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	c, err := Connect(mock.Addr())
	if err != nil {
		t.Fatal(err)
	}

	if err := c.Quit(); err != nil {
		t.Fatal(err)
	}
	mock.Wait()
}

func TestTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	if c, err := DialTimeout("localhost:2121", 1*time.Second); err == nil {
		_ = c.Quit()
		t.Fatal("expected timeout, got nil error")
	}
}

// TestTimeoutBoundsGreeting exercises upstream issue #256: a server that
// accepts the TCP connection but never sends the 220 greeting (a non-FTP
// service, a half-dead server) must not hang Dial forever — DialWithTimeout
// bounds the greeting read, not just the TCP connect.
func TestTimeoutBoundsGreeting(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- conn // hold it open, never send a greeting
	}()
	defer func() {
		select {
		case conn := <-accepted:
			_ = conn.Close()
		default:
		}
	}()

	start := time.Now()
	c, err := Dial(ln.Addr().String(), DialWithTimeout(500*time.Millisecond))
	if err == nil {
		_ = c.Quit()
		t.Fatal("expected a timeout error, got nil")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Dial did not time out promptly: %v", elapsed)
	}
}

func TestWrongLogin(t *testing.T) {
	mock, err := newFtpMock(t, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	c, err := DialTimeout(mock.Addr(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.Quit(); err != nil {
			t.Errorf("can not quit: %s", err)
		}
	}()

	err = c.Login("zoo2Shia", "fei5Yix9")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestLoginToleratesUTF8Refusal exercises upstream issue #356: a server that
// advertises UTF8 in FEAT but refuses "OPTS UTF8 ON" (e.g. wftpserver's
// "503 Send 'CLNT client_type' before enabling UTF8.") must not fail Login —
// UTF8 negotiation is cosmetic.
func TestLoginToleratesUTF8Refusal(t *testing.T) {
	mock, err := newFtpMockExt(t, "127.0.0.1", "no-time", func(m *ftpMock) {
		m.utf8Response = "503 Send 'CLNT client_type' before enabling UTF8."
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	c, err := DialTimeout(mock.Addr(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	if err := c.Login("anonymous", "anonymous"); err != nil {
		t.Fatalf("Login failed on a refused OPTS UTF8 ON: %s", err)
	}

	if err := c.Quit(); err != nil {
		t.Errorf("can not quit: %s", err)
	}
}

// TestLoginSendsCLNTWhenAdvertised: when the server lists CLNT in FEAT
// (wftpserver family), the client identifies itself before OPTS UTF8 ON so
// UTF8 is actually enabled rather than refused (upstream issue #356).
func TestLoginSendsCLNTWhenAdvertised(t *testing.T) {
	mock, err := newFtpMockExt(t, "127.0.0.1", "no-time", func(m *ftpMock) {
		m.featCLNT = true
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	c, err := DialTimeout(mock.Addr(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	if err := c.Login("anonymous", "anonymous"); err != nil {
		t.Fatal(err)
	}

	if err := c.Quit(); err != nil {
		t.Errorf("can not quit: %s", err)
	}

	mock.Wait()

	clntIdx, optsIdx := -1, -1
	for i, cmd := range mock.commands {
		switch cmd {
		case "CLNT":
			clntIdx = i
		case "OPTS":
			optsIdx = i
		}
	}
	if clntIdx == -1 {
		t.Fatalf("CLNT was not sent; commands: %v", mock.commands)
	}
	if optsIdx == -1 {
		t.Fatalf("OPTS was not sent; commands: %v", mock.commands)
	}
	if clntIdx > optsIdx {
		t.Errorf("CLNT (%d) must be sent before OPTS UTF8 ON (%d); commands: %v",
			clntIdx, optsIdx, mock.commands)
	}
}

// TestResponseCloseTolerates426 covers half of upstream issue #214: closing a
// download early makes the server report "426 Transfer aborted", which is the
// EXPECTED outcome of an early close, not an error.
func TestResponseCloseTolerates426(t *testing.T) {
	mock, err := newFtpMockExt(t, "127.0.0.1", "no-time", func(m *ftpMock) {
		m.retrFinal = "426 Transfer aborted. Link to file server lost."
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	c, err := DialTimeout(mock.Addr(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Login("anonymous", "anonymous"); err != nil {
		t.Fatal(err)
	}

	if err := c.Stor("f", bytes.NewBufferString("partial download content")); err != nil {
		t.Fatal(err)
	}

	r, err := c.Retr("f")
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1)
	if _, err := r.Read(buf); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close after a partial read must tolerate 426, got: %s", err)
	}

	if err := c.Quit(); err != nil {
		t.Fatal(err)
	}
	mock.Wait()
}

// TestResponseCloseDefaultDeadline covers the other half of upstream issue
// #214: a server that never reports the closing status must not hang
// Response.Close forever — the wait is bounded by DefaultShutTimeout even when
// no DialWithShutTimeout was configured.
func TestResponseCloseDefaultDeadline(t *testing.T) {
	oldDefault := DefaultShutTimeout
	DefaultShutTimeout = 300 * time.Millisecond
	defer func() { DefaultShutTimeout = oldDefault }()

	mock, err := newFtpMockExt(t, "127.0.0.1", "no-time", func(m *ftpMock) {
		m.retrFinal = "NONE"
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	c, err := DialTimeout(mock.Addr(), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Login("anonymous", "anonymous"); err != nil {
		t.Fatal(err)
	}

	if err := c.Stor("f", bytes.NewBufferString("content")); err != nil {
		t.Fatal(err)
	}

	r, err := c.Retr("f")
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	err = r.Close()
	if err == nil {
		t.Fatal("expected a timeout error from Close, got nil")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("Close did not time out promptly: %v", elapsed)
	}
}

// TestShutTimeoutDeadlineCleared: the shutTimeout deadline armed while reading
// a transfer's closing status must not stay on the control connection — left
// armed, it failed every later command once it expired.
func TestShutTimeoutDeadlineCleared(t *testing.T) {
	mock, c := openConn(t, "127.0.0.1", DialWithShutTimeout(300*time.Millisecond))

	if err := c.Stor("f", bytes.NewBufferString("content")); err != nil {
		t.Fatal(err)
	}

	// Let the shut deadline expire, then use the control connection again.
	time.Sleep(400 * time.Millisecond)

	if err := c.NoOp(); err != nil {
		t.Fatalf("control connection unusable after the shut deadline expired: %s", err)
	}

	if err := c.Quit(); err != nil {
		t.Fatal(err)
	}
	mock.Wait()
}

// TestSkipPASVAddress covers upstream issue #305: a NAT'd/misconfigured server
// advertises an address in the PASV reply that the client cannot reach, and
// whose class matches the control host so isBogusDataIP cannot catch it (here:
// 127.0.0.2 advertised, listener on 127.0.0.1 — both loopback). With
// DialWithSkipPASVAddress the data connection dials the control host instead
// and the transfer works; only the port is taken from the reply.
func TestSkipPASVAddress(t *testing.T) {
	mock, err := newFtpMockExt(t, "127.0.0.1", "no-time", func(m *ftpMock) {
		m.pasvHost = "127,0,0,2"
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	c, err := Dial(mock.Addr(),
		DialWithTimeout(5*time.Second),
		DialWithDisabledEPSV(true), // force PASV so the advertised host matters
		DialWithSkipPASVAddress(true),
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := c.Login("anonymous", "anonymous"); err != nil {
		t.Fatal(err)
	}

	// Any data-connection command proves the point: it only succeeds if the
	// client ignored 127.0.0.2 and dialed the control host.
	entries, err := c.List("")
	if err != nil {
		t.Fatalf("List over PASV with a bogus advertised host failed: %s", err)
	}
	if len(entries) == 0 {
		t.Error("expected list entries")
	}

	if err := c.Quit(); err != nil {
		t.Fatal(err)
	}
	mock.Wait()
}

// TestChmod covers the SITE CHMOD extension (upstream issue #450).
func TestChmod(t *testing.T) {
	mock, c := openConn(t, "127.0.0.1")

	if err := c.Chmod("file.txt", 0o644); err != nil {
		t.Fatal(err)
	}
	if mock.lastFull != "SITE CHMOD 644 file.txt" {
		t.Errorf("wire format %q, expected %q", mock.lastFull, "SITE CHMOD 644 file.txt")
	}

	// Only the permission bits go on the wire, whatever else the mode carries.
	if err := c.Chmod("dir", 0o2775|fs.ModeDir); err != nil {
		t.Fatal(err)
	}
	if mock.lastFull != "SITE CHMOD 775 dir" {
		t.Errorf("wire format %q, expected %q", mock.lastFull, "SITE CHMOD 775 dir")
	}

	if err := c.Quit(); err != nil {
		t.Fatal(err)
	}
	mock.Wait()
}

// TestQuote covers the raw-command passthrough (upstream issue #411).
func TestQuote(t *testing.T) {
	mock, c := openConn(t, "127.0.0.1")

	code, msg, err := c.Quote("NOOP")
	if err != nil {
		t.Fatal(err)
	}
	if code != StatusCommandOK {
		t.Errorf("code %d, expected %d", code, StatusCommandOK)
	}
	if msg == "" {
		t.Error("expected a non-empty response message")
	}

	// A refused command surfaces the server's code + text as a textproto.Error.
	code, _, err = c.Quote("BOGUS")
	if err == nil {
		t.Fatal("expected an error for an unknown command")
	}
	if code != StatusBadCommand {
		t.Errorf("code %d, expected %d", code, StatusBadCommand)
	}
	var protoErr *textproto.Error
	if !errors.As(err, &protoErr) {
		t.Errorf("expected a *textproto.Error, got %T", err)
	} else if protoErr.Code != StatusBadCommand {
		t.Errorf("textproto code %d, expected %d", protoErr.Code, StatusBadCommand)
	}

	if err := c.Quit(); err != nil {
		t.Fatal(err)
	}
	mock.Wait()
}

func TestDeleteDirRecur(t *testing.T) {
	mock, c := openConn(t, "127.0.0.1")

	err := c.RemoveDirRecur("testDir")
	if err != nil {
		t.Error(err)
	}

	if err := c.Quit(); err != nil {
		t.Fatal(err)
	}

	// Wait for the connection to close
	mock.Wait()
}

// func TestFileDeleteDirRecur(t *testing.T) {
// 	mock, c := openConn(t, "127.0.0.1")

// 	err := c.RemoveDirRecur("testFile")
// 	if err == nil {
// 		t.Fatal("expected error got nil")
// 	}

// 	if err := c.Quit(); err != nil {
// 		t.Fatal(err)
// 	}

// 	// Wait for the connection to close
// 	mock.Wait()
// }

func TestMissingFolderDeleteDirRecur(t *testing.T) {
	mock, c := openConn(t, "127.0.0.1")

	err := c.RemoveDirRecur("missing-dir")
	if err == nil {
		t.Fatal("expected error got nil")
	}

	if err := c.Quit(); err != nil {
		t.Fatal(err)
	}

	// Wait for the connection to close
	mock.Wait()
}

func TestListCurrentDir(t *testing.T) {
	mock, c := openConnExt(t, "127.0.0.1", "no-time", DialWithDisabledMLSD(true))

	_, err := c.List("")
	assert.NoError(t, err)
	assert.Equal(t, "LIST", mock.lastFull, "LIST must not have a trailing whitespace")

	_, err = c.NameList("")
	assert.NoError(t, err)
	assert.Equal(t, "NLST", mock.lastFull, "NLST must not have a trailing whitespace")

	err = c.Quit()
	assert.NoError(t, err)

	mock.Wait()
}

func TestListCurrentDirWithForceListHidden(t *testing.T) {
	mock, c := openConnExt(t, "127.0.0.1", "no-time", DialWithDisabledMLSD(true), DialWithForceListHidden(true))

	assert.True(t, c.options.forceListHidden)
	_, err := c.List("")
	assert.NoError(t, err)
	assert.Equal(t, "LIST -a", mock.lastFull, "LIST -a must not have a trailing whitespace")

	err = c.Quit()
	assert.NoError(t, err)

	mock.Wait()
}

func TestTimeUnsupported(t *testing.T) {
	mock, c := openConnExt(t, "127.0.0.1", "no-time")

	assert.False(t, c.mdtmSupported, "MDTM must NOT be supported")
	assert.False(t, c.mfmtSupported, "MFMT must NOT be supported")

	assert.False(t, c.IsGetTimeSupported(), "GetTime must NOT be supported")
	assert.False(t, c.IsSetTimeSupported(), "SetTime must NOT be supported")

	_, err := c.GetTime("file1")
	assert.NotNil(t, err)

	err = c.SetTime("file1", time.Now())
	assert.NotNil(t, err)

	assert.NoError(t, c.Quit())
	mock.Wait()
}

func TestTimeStandard(t *testing.T) {
	mock, c := openConnExt(t, "127.0.0.1", "std-time")

	assert.True(t, c.mdtmSupported, "MDTM must be supported")
	assert.True(t, c.mfmtSupported, "MFMT must be supported")

	assert.True(t, c.IsGetTimeSupported(), "GetTime must be supported")
	assert.True(t, c.IsSetTimeSupported(), "SetTime must be supported")

	tm, err := c.GetTime("file1")
	assert.NoError(t, err)
	assert.False(t, tm.IsZero(), "GetTime must return valid time")

	err = c.SetTime("file1", time.Now())
	assert.NoError(t, err)

	assert.NoError(t, c.Quit())
	mock.Wait()
}

func TestTimeVsftpdPartial(t *testing.T) {
	mock, c := openConnExt(t, "127.0.0.1", "vsftpd")

	assert.True(t, c.mdtmSupported, "MDTM must be supported")
	assert.False(t, c.mfmtSupported, "MFMT must NOT be supported")

	assert.True(t, c.IsGetTimeSupported(), "GetTime must be supported")
	assert.False(t, c.IsSetTimeSupported(), "SetTime must NOT be supported")

	tm, err := c.GetTime("file1")
	assert.NoError(t, err)
	assert.False(t, tm.IsZero(), "GetTime must return valid time")

	err = c.SetTime("file1", time.Now())
	assert.NotNil(t, err)

	assert.NoError(t, c.Quit())
	mock.Wait()
}

func TestTimeVsftpdFull(t *testing.T) {
	mock, c := openConnExt(t, "127.0.0.1", "vsftpd", DialWithWritingMDTM(true))

	assert.True(t, c.mdtmSupported, "MDTM must be supported")
	assert.False(t, c.mfmtSupported, "MFMT must NOT be supported")

	assert.True(t, c.IsGetTimeSupported(), "GetTime must be supported")
	assert.True(t, c.IsSetTimeSupported(), "SetTime must be supported")

	tm, err := c.GetTime("file1")
	assert.NoError(t, err)
	assert.False(t, tm.IsZero(), "GetTime must return valid time")

	err = c.SetTime("file1", time.Now())
	assert.NoError(t, err)

	assert.NoError(t, c.Quit())
	mock.Wait()
}

func TestDialWithDialFunc(t *testing.T) {
	dialErr := fmt.Errorf("this is proof that dial function was called")

	f := func(network, address string) (net.Conn, error) {
		return nil, dialErr
	}

	_, err := Dial("bogus-address", DialWithDialFunc(f))
	assert.Equal(t, dialErr, err)
}

func TestDialWithDialer(t *testing.T) {
	dialerCalled := false
	dialer := net.Dialer{
		Control: func(network, address string, c syscall.RawConn) error {
			dialerCalled = true
			return nil
		},
	}

	mock, err := newFtpMock(t, "127.0.0.1")
	assert.NoError(t, err)

	c, err := Dial(mock.Addr(), DialWithDialer(dialer))
	assert.NoError(t, err)
	assert.NoError(t, c.Quit())

	assert.Equal(t, true, dialerCalled)
}

// TestStrictDataTransfers426IsError is the pull-consumer counterpart of
// TestResponseCloseTolerates426: a consumer that always reads to EOF opts in
// via DialWithStrictDataTransfers, and for it a final "426 Transfer aborted"
// means the transfer was genuinely truncated — Close MUST surface it. Without
// this, io.Copy sees a clean EOF and a short file reads as a success.
func TestStrictDataTransfers426IsError(t *testing.T) {
	mock, err := newFtpMockExt(t, "127.0.0.1", "no-time", func(m *ftpMock) {
		m.retrFinal = "426 Transfer aborted. Link to file server lost."
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mock.Close()

	c, err := Dial(mock.Addr(), DialWithTimeout(5*time.Second), DialWithStrictDataTransfers(true))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Login("anonymous", "anonymous"); err != nil {
		t.Fatal(err)
	}

	if err := c.Stor("f", bytes.NewBufferString("truncated download content")); err != nil {
		t.Fatal(err)
	}

	r, err := c.Retr("f")
	if err != nil {
		t.Fatal(err)
	}
	// Read to EOF like a real pull consumer — the mock still finishes with 426.
	if _, err := io.ReadAll(r); err != nil {
		t.Fatal(err)
	}
	err = r.Close()
	if err == nil {
		t.Fatal("strict mode: Close after a 426 completion must be an error")
	}
	var tperr *textproto.Error
	if !errors.As(err, &tperr) || tperr.Code != StatusTransfertAborted {
		t.Fatalf("want textproto 426, got: %v", err)
	}

	if err := c.Quit(); err != nil {
		t.Fatal(err)
	}
}

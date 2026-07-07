package ftp

import (
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ftpMock struct {
	t            *testing.T
	address      string
	modtime      string // no-time, std-time, vsftpd
	utf8Response string // non-empty overrides the "OPTS UTF8 ON" reply (e.g. a refusal)
	featCLNT     bool   // advertise CLNT in FEAT (wftpserver family)
	pasvHost     string // non-empty advertises this comma-form host in the PASV reply (NAT misconfig)
	retrFinal    string // RETR closing status: "" = 226, "NONE" = say nothing, else sent verbatim
	listener     *net.TCPListener
	proto        *textproto.Conn
	commands     []string // list of received commands
	lastFull     string   // full last command
	rest         int
	fileCont     *bytes.Buffer
	dataConn     *mockDataConn
	sync.WaitGroup
}

// newFtpMock returns a mock implementation of a FTP server
// For simplication, a mock instance only accepts a signle connection and terminates afer
func newFtpMock(t *testing.T, address string) (*ftpMock, error) {
	return newFtpMockExt(t, address, "no-time")
}

// newFtpMockExt builds and starts the mock. Optional opts run on the mock
// BEFORE the listener goroutine starts, so tests can set behaviour knobs
// without racing the connection handler.
func newFtpMockExt(t *testing.T, address, modtime string, opts ...func(*ftpMock)) (*ftpMock, error) {
	var err error
	mock := &ftpMock{
		t:       t,
		address: address,
		modtime: modtime,
	}

	for _, opt := range opts {
		opt(mock)
	}

	l, err := net.Listen("tcp", address+":0")
	if err != nil {
		return nil, err
	}

	tcpListener, ok := l.(*net.TCPListener)
	if !ok {
		return nil, errors.New("listener is not a net.TCPListener")
	}
	mock.listener = tcpListener

	go mock.listen()

	return mock, nil
}

func (mock *ftpMock) listen() {
	// Listen for an incoming connection.
	conn, err := mock.listener.Accept()
	if err != nil {
		mock.t.Errorf("can not accept: %s", err)
		return
	}

	mock.Add(1)
	defer func() {
		assert.NoError(mock.t, conn.Close(), "closing conn after listen")
		mock.Done()
	}()

	mock.proto = textproto.NewConn(conn)
	mock.printfLine("220 FTP Server ready.")

	for {
		fullCommand, _ := mock.proto.ReadLine()
		mock.lastFull = fullCommand

		cmdParts := strings.Split(fullCommand, " ")

		// Append to list of received commands
		mock.commands = append(mock.commands, cmdParts[0])

		// At least one command must have a multiline response
		switch cmdParts[0] {
		case "FEAT":
			features := "211-Features:\r\n FEAT\r\n PASV\r\n EPSV\r\n UTF8\r\n SIZE\r\n MLST\r\n"
			switch mock.modtime {
			case "std-time":
				features += " MDTM\r\n MFMT\r\n"
			case "vsftpd":
				features += " MDTM\r\n"
			}
			if mock.featCLNT {
				features += " CLNT\r\n"
			}
			features += "211 End"
			mock.printfLine("%s", features)
		case "CLNT":
			mock.printfLine("200 Noted.")
		case "USER":
			if cmdParts[1] == "anonymous" {
				mock.printfLine("331 Please send your password")
			} else {
				mock.printfLine("530 This FTP server is anonymous only")
			}
		case "PASS":
			mock.printfLine("230-Hey,\r\nWelcome to my FTP\r\n230 Access granted")
		case "TYPE":
			mock.printfLine("200 Type set ok")
		case "CWD":
			if cmdParts[1] == "missing-dir" {
				mock.printfLine("550 %s: No such file or directory", cmdParts[1])
			} else {
				mock.printfLine("250 Directory successfully changed.")
			}
		case "DELE":
			mock.printfLine("250 File successfully removed.")
		case "MKD":
			mock.printfLine("257 Directory successfully created.")
		case "RMD":
			if cmdParts[1] == "missing-dir" {
				mock.printfLine("550 No such file or directory")
			} else {
				mock.printfLine("250 Directory successfully removed.")
			}
		case "PWD":
			mock.printfLine("257 \"/incoming\"")
		case "CDUP":
			mock.printfLine("250 CDUP command successful")
		case "SIZE":
			if cmdParts[1] == "magic-file" {
				mock.printfLine("213 42")
			} else {
				mock.printfLine("550 Could not get file size.")
			}
		case "PASV":
			p, err := mock.listenDataConn()
			if err != nil {
				mock.printfLine("451 %s.", err)
				break
			}

			p1 := int(p / 256)
			p2 := p % 256

			pasvHost := mock.pasvHost
			if pasvHost == "" {
				pasvHost = "127,0,0,1"
			}
			mock.printfLine("227 Entering Passive Mode (%s,%d,%d).", pasvHost, p1, p2)
		case "EPSV":
			p, err := mock.listenDataConn()
			if err != nil {
				mock.printfLine("451 %s.", err)
				break
			}
			mock.printfLine("229 Entering Extended Passive Mode (|||%d|)", p)
		case "STOR":
			if mock.dataConn == nil {
				mock.printfLine("425 Unable to build data connection: Connection refused")
				break
			}
			mock.printfLine("150 please send")
			mock.recvDataConn(false)
		case "APPE":
			if mock.dataConn == nil {
				mock.printfLine("425 Unable to build data connection: Connection refused")
				break
			}
			mock.printfLine("150 please send")
			mock.recvDataConn(true)
		case "LIST":
			if mock.dataConn == nil {
				mock.printfLine("425 Unable to build data connection: Connection refused")
				break
			}

			mock.dataConn.Wait()
			mock.printfLine("150 Opening ASCII mode data connection for file list")
			mock.dataConn.write([]byte("-rw-r--r--   1 ftp      wheel           0 Jan 29 10:29 lo\r\ntotal 1"))
			mock.printfLine("226 Transfer complete")
			mock.closeDataConn()
		case "MLSD":
			if mock.dataConn == nil {
				mock.printfLine("425 Unable to build data connection: Connection refused")
				break
			}

			mock.dataConn.Wait()
			mock.printfLine("150 Opening data connection for file list")
			mock.dataConn.write([]byte("Type=file;Size=0;Modify=20201213202400; lo\r\n"))
			mock.printfLine("226 Transfer complete")
			mock.closeDataConn()
		case "MLST":
			if cmdParts[1] == "multiline-dir" {
				mock.printfLine("250-File data\r\n Type=dir;Size=0; multiline-dir\r\n Modify=20201213202400; multiline-dir\r\n250 End")
			} else if cmdParts[1] == "leading-space-file" {
				// Some servers (e.g. WS_FTP 8.6.1) pad the entry line with
				// several leading spaces instead of the single space RFC 3659
				// mandates (upstream issue #338).
				mock.printfLine("250-File data\r\n    Type=file;Size=42;Modify=20201213202400; leading-space-file\r\n250 End")
			} else {
				mock.printfLine("250-File data\r\n Type=file;Size=42;Modify=20201213202400; magic-file\r\n \r\n250 End")
			}
		case "NLST":
			if mock.dataConn == nil {
				mock.printfLine("425 Unable to build data connection: Connection refused")
				break
			}

			mock.dataConn.Wait()
			mock.printfLine("150 Opening ASCII mode data connection for file list")
			mock.dataConn.write([]byte("/incoming"))
			mock.printfLine("226 Transfer complete")
			mock.closeDataConn()
		case "RETR":
			if mock.dataConn == nil {
				mock.printfLine("425 Unable to build data connection: Connection refused")
				break
			}

			mock.dataConn.Wait()
			mock.printfLine("150 Opening ASCII mode data connection for file list")
			mock.dataConn.write(mock.fileCont.Bytes()[mock.rest:])
			mock.rest = 0
			switch mock.retrFinal {
			case "":
				mock.printfLine("226 Transfer complete")
			case "NONE":
				// Say nothing: simulates a server that never reports the
				// closing status (upstream issue #214).
			default:
				mock.printfLine("%s", mock.retrFinal)
			}
			mock.closeDataConn()
		case "RNFR":
			mock.printfLine("350 File or directory exists, ready for destination name")
		case "RNTO":
			mock.printfLine("250 Rename successful")
		case "REST":
			if len(cmdParts) != 2 {
				mock.printfLine("500 wrong number of arguments")
				break
			}
			rest, err := strconv.Atoi(cmdParts[1])
			if err != nil {
				mock.printfLine("500 REST: %s", err)
				break
			}
			mock.rest = rest
			mock.printfLine("350 Restarting at %s. Send STORE or RETRIEVE to initiate transfer", cmdParts[1])
		case "MDTM":
			var answer string
			switch {
			case mock.modtime == "no-time":
				answer = "500 Unknown command MDTM"
			case len(cmdParts) == 3 && mock.modtime == "vsftpd":
				answer = "213 UTIME OK"
				_, err := time.ParseInLocation(timeFormat, cmdParts[1], time.UTC)
				if err != nil {
					answer = "501 Can't get a time stamp"
				}
			case len(cmdParts) == 2:
				answer = "213 20201213202400"
			default:
				answer = "500 wrong number of arguments"
			}
			mock.printfLine("%s", answer)
		case "MFMT":
			var answer string
			switch {
			case mock.modtime == "std-time" && len(cmdParts) == 3:
				answer = "213 UTIME OK"
				_, err := time.ParseInLocation(timeFormat, cmdParts[1], time.UTC)
				if err != nil {
					answer = "501 Can't get a time stamp"
				}
			default:
				answer = "500 Unknown command MFMT"
			}
			mock.printfLine("%s", answer)
		case "NOOP":
			mock.printfLine("200 NOOP ok.")
		case "SITE":
			// Accept exactly the wire form Chmod emits: SITE CHMOD <octal> <path>
			if len(cmdParts) == 4 && cmdParts[1] == "CHMOD" {
				if _, err := strconv.ParseUint(cmdParts[2], 8, 32); err == nil {
					mock.printfLine("200 SITE CHMOD command successful.")
					break
				}
			}
			mock.printfLine("500 'SITE %s': command not understood.", strings.Join(cmdParts[1:], " "))
		case "OPTS":
			if len(cmdParts) != 3 {
				mock.printfLine("500 wrong number of arguments")
				break
			}
			if (strings.Join(cmdParts[1:], " ")) == "UTF8 ON" {
				if mock.utf8Response != "" {
					mock.printfLine("%s", mock.utf8Response)
				} else {
					mock.printfLine("200 OK, UTF-8 enabled")
				}
			}
		case "REIN":
			mock.printfLine("220 Logged out")
		case "QUIT":
			mock.printfLine("221 Goodbye.")
			return
		default:
			mock.printfLine("500 Unknown command %s.", cmdParts[0])
		}
	}
}

// TestSecureDataConn exercises upstream issue #425: with a custom dial func +
// explicit TLS, data connections must be TLS-wrapped by the library (the
// control connection already is, via the AUTH TLS upgrade in Dial). Implicit
// mode and non-TLS setups must pass the dial func's connection through
// untouched.
func TestSecureDataConn(t *testing.T) {
	a, b := net.Pipe()
	defer func() {
		assert.NoError(t, a.Close())
		assert.NoError(t, b.Close())
	}()

	dialFunc := func(network, address string) (net.Conn, error) { return nil, nil }
	tlsConfig := &tls.Config{InsecureSkipVerify: true} //nolint:gosec // test fixture, no real peer

	// explicit TLS + dial func: the library owns the TLS upgrade -> wrapped
	wrapped := secureDataConn(a, &dialOptions{dialFunc: dialFunc, tlsConfig: tlsConfig, explicitTLS: true})
	if _, ok := wrapped.(*tls.Conn); !ok {
		t.Errorf("explicit TLS + dialFunc: expected *tls.Conn, got %T", wrapped)
	}

	// implicit TLS + dial func: the dial func owns TLS -> untouched
	if got := secureDataConn(a, &dialOptions{dialFunc: dialFunc, tlsConfig: tlsConfig, explicitTLS: false}); got != a {
		t.Errorf("implicit TLS + dialFunc: expected the conn untouched, got %T", got)
	}

	// no TLS config -> untouched
	if got := secureDataConn(a, &dialOptions{dialFunc: dialFunc, explicitTLS: true}); got != a {
		t.Errorf("no tlsConfig: expected the conn untouched, got %T", got)
	}

	// no dial func -> untouched (the regular openDataConn TLS branch handles it)
	if got := secureDataConn(a, &dialOptions{tlsConfig: tlsConfig, explicitTLS: true}); got != a {
		t.Errorf("no dialFunc: expected the conn untouched, got %T", got)
	}

	// nil conn passes through
	if got := secureDataConn(nil, &dialOptions{}); got != nil {
		t.Errorf("nil conn: expected nil, got %T", got)
	}
}

func (mock *ftpMock) printfLine(format string, args ...interface{}) {
	if err := mock.proto.PrintfLine(format, args...); err != nil {
		mock.t.Fatal(err)
	}
}

func (mock *ftpMock) closeDataConn() {
	if mock.dataConn != nil {
		if err := mock.dataConn.Close(); err != nil {
			mock.t.Fatal(err)
		}
		mock.dataConn = nil
	}
}

type mockDataConn struct {
	t        *testing.T
	listener *net.TCPListener
	conn     net.Conn
	// WaitGroup is done when conn is accepted and stored
	sync.WaitGroup
}

func (d *mockDataConn) Close() (err error) {
	if d.listener != nil {
		err = d.listener.Close()
	}
	if d.conn != nil {
		err = d.conn.Close()
	}
	return
}

func (d *mockDataConn) write(b []byte) {
	if d.conn == nil {
		d.t.Fatal("data conn is not opened")
	}

	if _, err := d.conn.Write(b); err != nil {
		d.t.Fatal(err)
	}
}

func (mock *ftpMock) listenDataConn() (int64, error) {
	mock.closeDataConn()

	l, err := net.Listen("tcp", mock.address+":0")
	if err != nil {
		return 0, err
	}

	tcpListener, ok := l.(*net.TCPListener)
	if !ok {
		return 0, errors.New("listener is not a net.TCPListener")
	}

	addr := tcpListener.Addr().String()

	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, err
	}

	p, err := strconv.ParseInt(port, 10, 32)
	if err != nil {
		return 0, err
	}

	dataConn := &mockDataConn{
		t:        mock.t,
		listener: tcpListener,
	}
	dataConn.Add(1)

	go func() {
		// Listen for an incoming connection.
		conn, err := dataConn.listener.Accept()
		if err != nil {
			// mock.t.Fatalf("can not accept data conn: %s", err)
			return
		}

		dataConn.conn = conn
		dataConn.Done()
	}()

	mock.dataConn = dataConn
	return p, nil
}

func (mock *ftpMock) recvDataConn(append bool) {
	mock.dataConn.Wait()
	if !append {
		mock.fileCont = new(bytes.Buffer)
	}

	if _, err := io.Copy(mock.fileCont, mock.dataConn.conn); err != nil {
		mock.t.Fatal(err)
	}

	mock.printfLine("226 Transfer Complete")
	mock.closeDataConn()
}

func (mock *ftpMock) Addr() string {
	return mock.listener.Addr().String()
}

// Closes the listening socket
func (mock *ftpMock) Close() {
	assert.NoError(mock.t, mock.listener.Close(), "closing listener")
}

// Helper to return a client connected to a mock server
func openConn(t *testing.T, addr string, options ...DialOption) (*ftpMock, *ServerConn) {
	return openConnExt(t, addr, "no-time", options...)
}

func openConnExt(t *testing.T, addr, modtime string, options ...DialOption) (*ftpMock, *ServerConn) {
	mock, err := newFtpMockExt(t, addr, modtime)
	require.NoError(t, err)
	defer mock.Close()

	c, err := Dial(mock.Addr(), options...)
	require.NoError(t, err)

	err = c.Login("anonymous", "anonymous")
	require.NoError(t, err)

	return mock, c
}

// Helper to close a client connected to a mock server
func closeConn(t *testing.T, mock *ftpMock, c *ServerConn, commands []string) {
	expected := []string{"USER", "PASS", "FEAT", "TYPE", "OPTS"}
	expected = append(expected, commands...)
	expected = append(expected, "QUIT")

	if err := c.Quit(); err != nil {
		t.Fatal(err)
	}

	// Wait for the connection to close
	mock.Wait()

	assert.Equal(t, expected, mock.commands, "unexpected sequence of commands")
}

func TestConn4(t *testing.T) {
	mock, c := openConn(t, "127.0.0.1")
	closeConn(t, mock, c, nil)
}

func TestConn6(t *testing.T) {
	mock, c := openConn(t, "[::1]")
	closeConn(t, mock, c, nil)
}

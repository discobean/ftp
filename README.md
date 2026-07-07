# goftp #

[![Units tests](https://github.com/jlaffaye/ftp/actions/workflows/unit_tests.yaml/badge.svg)](https://github.com/jlaffaye/ftp/actions/workflows/unit_tests.yaml)
[![Coverage Status](https://coveralls.io/repos/jlaffaye/ftp/badge.svg?branch=master&service=github)](https://coveralls.io/github/jlaffaye/ftp?branch=master)
[![golangci-lint](https://github.com/jlaffaye/ftp/actions/workflows/golangci-lint.yaml/badge.svg)](https://github.com/jlaffaye/ftp/actions/workflows/golangci-lint.yaml)
[![Go ReportCard](https://goreportcard.com/badge/jlaffaye/ftp)](http://goreportcard.com/report/jlaffaye/ftp)
[![Go Reference](https://pkg.go.dev/badge/github.com/jlaffaye/ftp.svg)](https://pkg.go.dev/github.com/jlaffaye/ftp)

A FTP client package for Go

## Install ##

```
go get -u github.com/jlaffaye/ftp
```

## Documentation ##

https://pkg.go.dev/github.com/jlaffaye/ftp

## Example ##

```go
c, err := ftp.Dial("ftp.example.org:21", ftp.DialWithTimeout(5*time.Second))
if err != nil {
    log.Fatal(err)
}

err = c.Login("anonymous", "anonymous")
if err != nil {
    log.Fatal(err)
}

// Do something with the FTP conn

if err := c.Quit(); err != nil {
    log.Fatal(err)
}
```

## Store a file example ##

```go
data := bytes.NewBufferString("Hello World")
err = c.Stor("test-file.txt", data)
if err != nil {
	panic(err)
}
```

## Read a file example ##

```go
r, err := c.Retr("test-file.txt")
if err != nil {
	panic(err)
}
defer r.Close()

buf, err := ioutil.ReadAll(r)
println(string(buf))
```

## Error handling ##

Errors at the protocol level — every non-success reply from the server — are
returned as a [`*textproto.Error`](https://pkg.go.dev/net/textproto#Error),
whose `Code` field carries the three-digit FTP reply code and `Msg` the
server's text. Use `errors.As` to detect the cause of a failure:

```go
err = c.Login("user", "wrong-password")

var protoErr *textproto.Error
if errors.As(err, &protoErr) {
    switch protoErr.Code {
    case ftp.StatusNotLoggedIn: // 530
        log.Fatal("bad credentials")
    case ftp.StatusFileUnavailable: // 550
        log.Fatal("no such file or permission denied")
    default:
        log.Fatalf("server refused: %d %s", protoErr.Code, protoErr.Msg)
    }
}
```

The `ftp.Status*` constants (see `status.go`) name every standard reply code,
so comparisons never need magic numbers. Errors that are not
`*textproto.Error` are transport-level failures (dial, timeout, TLS,
connection reset) from the underlying `net` layer and can be inspected the
usual way (`errors.As` with `net.Error`, `errors.Is(err, os.ErrDeadlineExceeded)`,
…).

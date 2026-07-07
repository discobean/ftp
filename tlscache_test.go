package ftp

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"
)

// TestEnsureTLSSessionCache covers the config handling for upstream issues
// #342/#435/#323: a tls.Config without a ClientSessionCache is cloned and
// given one (never mutating the caller's config); a caller-supplied cache is
// respected.
func TestEnsureTLSSessionCache(t *testing.T) {
	// nil config: nothing to do
	do := &dialOptions{}
	ensureTLSSessionCache(do)
	if do.tlsConfig != nil {
		t.Fatal("nil tlsConfig must stay nil")
	}

	// no cache: cloned + cache installed, original untouched
	orig := &tls.Config{ServerName: "ftps.example.com"} //nolint:gosec // test fixture
	do = &dialOptions{tlsConfig: orig}
	ensureTLSSessionCache(do)
	if do.tlsConfig == orig {
		t.Fatal("config must be cloned, not mutated in place")
	}
	if do.tlsConfig.ClientSessionCache == nil {
		t.Fatal("a session cache must be installed")
	}
	if do.tlsConfig.ServerName != "ftps.example.com" {
		t.Fatal("clone must preserve the caller's fields")
	}
	if orig.ClientSessionCache != nil {
		t.Fatal("the caller's config must not be mutated")
	}

	// caller-supplied cache: left exactly as-is
	cache := tls.NewLRUClientSessionCache(1)
	orig = &tls.Config{ClientSessionCache: cache}
	do = &dialOptions{tlsConfig: orig}
	ensureTLSSessionCache(do)
	if do.tlsConfig != orig {
		t.Fatal("a config with a cache must be left untouched")
	}
	if do.tlsConfig.ClientSessionCache != cache {
		t.Fatal("the caller's cache must be preserved")
	}
}

// TestSessionCacheEnablesResumption proves the injected cache makes a second
// handshake against the same server RESUME the first one's session — the
// control-connection-then-data-connection pattern that FileZilla Server,
// proftpd (RequireSessionReuse) and pure-ftpd demand.
func TestSessionCacheEnablesResumption(t *testing.T) {
	cert := genTestCert(t, "ftps.example.com")
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{cert},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				// Complete the handshake and give the client a moment to
				// receive the TLS 1.3 session ticket before closing.
				tc := c.(*tls.Conn)
				_ = tc.Handshake()
				_, _ = tc.Write([]byte("x"))
				time.Sleep(50 * time.Millisecond)
				_ = tc.Close()
			}(conn)
		}
	}()

	// The exact path Dial takes: caller config without a cache, run through
	// ensureTLSSessionCache, then reused for a second (data) connection.
	do := &dialOptions{tlsConfig: &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // self-signed test server
		ServerName:         "ftps.example.com",
	}}
	ensureTLSSessionCache(do)

	dial := func() tls.ConnectionState {
		conn, err := tls.Dial("tcp", ln.Addr().String(), do.tlsConfig)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		// Read so the client processes the server's session ticket
		// (TLS 1.3 delivers it as a post-handshake message).
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
		return conn.ConnectionState()
	}

	if first := dial(); first.DidResume {
		t.Fatal("first connection cannot be a resumption")
	}
	if second := dial(); !second.DidResume {
		t.Fatal("second connection must resume the first one's session")
	}
}

func genTestCert(t *testing.T, host string) tls.Certificate {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: host},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:              []string{host},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}
}

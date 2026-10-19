package sqt

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/kirill-scherba/sqt/internal/devtls"
)

func testServer(t *testing.T) (*Server, context.Context, context.CancelFunc) {
	t.Helper()

	cert, _, err := devtls.SelfSigned()
	if err != nil {
		t.Fatalf("self-signed certificate: %v", err)
	}

	srv, err := NewServer(ServerConfig{
		Addr: "127.0.0.1:0",
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS13,
		},
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(func() {
		cancel()
		_ = srv.Close()
	})

	go func() {
		if err := srv.Run(ctx); err != nil {
			t.Logf("server run: %v", err)
		}
	}()

	return srv, ctx, cancel
}

func dialTest(t *testing.T, ctx context.Context, srv *Server) *Peer {
	t.Helper()

	peer, err := Dial(ctx, ClientConfig{
		Addr: srv.Addr().String(),
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS13,
		},
	})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = peer.Close() })

	return peer
}

func receive(t *testing.T, ch <-chan []byte) []byte {
	t.Helper()

	select {
	case msg, ok := <-ch:
		if !ok {
			t.Fatal("messages channel closed")
		}
		return msg
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a message")
	}
	return nil
}

func TestExchange(t *testing.T) {
	srv, ctx, _ := testServer(t)
	client := dialTest(t, ctx, srv)

	var serverPeer *Peer
	select {
	case serverPeer = <-srv.Peers():
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the server to accept the connection")
	}

	if serverPeer.ID() == "" {
		t.Fatal("server assigned an empty peer id")
	}

	if err := client.Send(ctx, []byte("hello")); err != nil {
		t.Fatalf("client send: %v", err)
	}
	if got := string(receive(t, serverPeer.Messages())); got != "hello" {
		t.Fatalf("server received %q, want %q", got, "hello")
	}

	if err := serverPeer.Send(ctx, []byte("world")); err != nil {
		t.Fatalf("server send: %v", err)
	}
	if got := string(receive(t, client.Messages())); got != "world" {
		t.Fatalf("client received %q, want %q", got, "world")
	}
}

func TestMultipleMessagesKeepOrder(t *testing.T) {
	srv, ctx, _ := testServer(t)
	client := dialTest(t, ctx, srv)

	var serverPeer *Peer
	select {
	case serverPeer = <-srv.Peers():
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the server to accept the connection")
	}

	const count = 20
	for i := range count {
		msg := []byte{byte(i)}
		if err := client.Send(ctx, msg); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}

	for i := range count {
		got := receive(t, serverPeer.Messages())
		if len(got) != 1 || got[0] != byte(i) {
			t.Fatalf("message %d: got %v", i, got)
		}
	}
}

func TestMessageTooLarge(t *testing.T) {
	srv, ctx, _ := testServer(t)
	client := dialTest(t, ctx, srv)

	select {
	case <-srv.Peers():
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the server to accept the connection")
	}

	err := client.Send(ctx, make([]byte, MaxMessageSize+1))
	if err != ErrMessageTooLarge {
		t.Fatalf("got %v, want ErrMessageTooLarge", err)
	}
}

func TestPeerClosedAfterServerClose(t *testing.T) {
	srv, ctx, _ := testServer(t)
	client := dialTest(t, ctx, srv)

	select {
	case <-srv.Peers():
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the server to accept the connection")
	}

	if err := srv.Close(); err != nil {
		t.Fatalf("server close: %v", err)
	}

	select {
	case <-client.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("client was not notified about the server shutdown")
	}

	if err := client.Err(); err == nil {
		t.Fatal("expected a close reason after the server shutdown")
	}
}

func TestLocalCloseHasNoError(t *testing.T) {
	srv, ctx, _ := testServer(t)
	client := dialTest(t, ctx, srv)

	select {
	case <-srv.Peers():
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the server to accept the connection")
	}

	if err := client.Close(); err != nil {
		t.Fatalf("client close: %v", err)
	}

	select {
	case <-client.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("client was not marked as closed")
	}

	if err := client.Err(); err != nil {
		t.Fatalf("local close reported an error: %v", err)
	}
}

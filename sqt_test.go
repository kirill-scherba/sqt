package sqt

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/kirill-scherba/sqt/internal/devtls"
)

var testNetworks = []string{NetworkQUIC, NetworkTCP}

func forEachNetwork(t *testing.T, fn func(t *testing.T, network string)) {
	t.Helper()
	for _, network := range testNetworks {
		t.Run(network, func(t *testing.T) { fn(t, network) })
	}
}

func testServer(t *testing.T, network string) (*Server, context.Context, context.CancelFunc) {
	t.Helper()

	cert, _, err := devtls.SelfSigned()
	if err != nil {
		t.Fatalf("self-signed certificate: %v", err)
	}

	srv, err := NewServer(ServerConfig{
		Addr:    "127.0.0.1:0",
		Network: network,
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

func dialTest(t *testing.T, ctx context.Context, srv *Server, network string) *Peer {
	t.Helper()

	peer, err := Dial(ctx, ClientConfig{
		Addr:    srv.Addr().String(),
		Network: network,
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

func acceptPeer(t *testing.T, srv *Server) *Peer {
	t.Helper()

	select {
	case p := <-srv.Peers():
		return p
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the server to accept the connection")
	}
	return nil
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
	forEachNetwork(t, func(t *testing.T, network string) {
		srv, ctx, _ := testServer(t, network)
		client := dialTest(t, ctx, srv, network)
		serverPeer := acceptPeer(t, srv)

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
	})
}

func TestMultipleMessagesKeepOrder(t *testing.T) {
	forEachNetwork(t, func(t *testing.T, network string) {
		srv, ctx, _ := testServer(t, network)
		client := dialTest(t, ctx, srv, network)
		serverPeer := acceptPeer(t, srv)

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
	})
}

func TestConcurrentSends(t *testing.T) {
	forEachNetwork(t, func(t *testing.T, network string) {
		srv, ctx, _ := testServer(t, network)
		client := dialTest(t, ctx, srv, network)
		serverPeer := acceptPeer(t, srv)

		const senders = 8
		const perSender = 5

		var wg sync.WaitGroup
		for i := range senders {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for j := range perSender {
					msg := fmt.Sprintf("%d-%d", i, j)
					if err := client.Send(ctx, []byte(msg)); err != nil {
						t.Errorf("send %s: %v", msg, err)
						return
					}
				}
			}(i)
		}
		wg.Wait()

		seen := make(map[string]bool)
		for range senders * perSender {
			seen[string(receive(t, serverPeer.Messages()))] = true
		}
		if len(seen) != senders*perSender {
			t.Fatalf("got %d distinct messages, want %d", len(seen), senders*perSender)
		}
	})
}

func TestMessageTooLarge(t *testing.T) {
	forEachNetwork(t, func(t *testing.T, network string) {
		srv, ctx, _ := testServer(t, network)
		client := dialTest(t, ctx, srv, network)
		acceptPeer(t, srv)

		err := client.Send(ctx, make([]byte, MaxMessageSize+1))
		if err != ErrMessageTooLarge {
			t.Fatalf("got %v, want ErrMessageTooLarge", err)
		}
	})
}

func TestPeerClosedAfterServerClose(t *testing.T) {
	forEachNetwork(t, func(t *testing.T, network string) {
		srv, ctx, _ := testServer(t, network)
		client := dialTest(t, ctx, srv, network)
		acceptPeer(t, srv)

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
	})
}

func TestLocalCloseHasNoError(t *testing.T) {
	forEachNetwork(t, func(t *testing.T, network string) {
		srv, ctx, _ := testServer(t, network)
		client := dialTest(t, ctx, srv, network)
		acceptPeer(t, srv)

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
	})
}

func TestUnknownNetwork(t *testing.T) {
	if _, err := NewServer(ServerConfig{Addr: ":0", Network: "sctp"}); err != ErrUnknownNetwork {
		t.Fatalf("server: got %v, want ErrUnknownNetwork", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := Dial(ctx, ClientConfig{Addr: "127.0.0.1:1", Network: "sctp"}); err != ErrUnknownNetwork {
		t.Fatalf("dial: got %v, want ErrUnknownNetwork", err)
	}
}

func TestHeartbeatKeepsIdleConnection(t *testing.T) {
	oldInterval, oldTimeout := heartbeatInterval, heartbeatTimeout
	heartbeatInterval = 100 * time.Millisecond
	heartbeatTimeout = 250 * time.Millisecond
	t.Cleanup(func() {
		heartbeatInterval = oldInterval
		heartbeatTimeout = oldTimeout
	})

	forEachNetwork(t, func(t *testing.T, network string) {
		srv, ctx, _ := testServer(t, network)
		client := dialTest(t, ctx, srv, network)
		serverPeer := acceptPeer(t, srv)

		// Several heartbeat cycles with no application traffic.
		time.Sleep(1200 * time.Millisecond)

		select {
		case <-client.Done():
			t.Fatalf("client closed: %v", client.Err())
		case <-serverPeer.Done():
			t.Fatalf("server peer closed: %v", serverPeer.Err())
		default:
		}

		select {
		case msg := <-client.Messages():
			t.Fatalf("heartbeat frame leaked into messages: %q", msg)
		default:
		}

		if err := client.Send(ctx, []byte("still alive")); err != nil {
			t.Fatalf("send after idle: %v", err)
		}
		if got := string(receive(t, serverPeer.Messages())); got != "still alive" {
			t.Fatalf("got %q, want %q", got, "still alive")
		}
	})
}

func TestClientReconnects(t *testing.T) {
	forEachNetwork(t, func(t *testing.T, network string) {
		srv, ctx, _ := testServer(t, network)

		client := NewClient(ClientConfig{
			Addr:    srv.Addr().String(),
			Network: network,
			TLSConfig: &tls.Config{
				InsecureSkipVerify: true,
				MinVersion:         tls.VersionTLS13,
			},
		})

		handlers := make(chan struct{}, 4)
		go func() {
			_ = client.Run(ctx, func(ctx context.Context, p *Peer) {
				handlers <- struct{}{}
				<-p.Done()
			})
		}()

		first := acceptPeer(t, srv)
		waitSignal(t, handlers, "first handler")

		if err := first.Close(); err != nil {
			t.Fatalf("close first peer: %v", err)
		}

		second := acceptPeer(t, srv)
		if second.ID() == first.ID() {
			t.Fatalf("expected a new peer id, got %s", second.ID())
		}
		waitSignal(t, handlers, "second handler")
	})
}

func waitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

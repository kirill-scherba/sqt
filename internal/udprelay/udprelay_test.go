package udprelay

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func echoServer(t *testing.T) *net.UDPConn {
	t.Helper()

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen echo: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	go func() {
		buf := make([]byte, 64*1024)
		for {
			n, addr, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			_, _ = conn.WriteToUDP(append([]byte("echo:"), buf[:n]...), addr)
		}
	}()

	return conn
}

func TestRelayForwardsBothWays(t *testing.T) {
	echo := echoServer(t)

	relay, err := New(Config{
		ListenAddr:   "127.0.0.1:0",
		UpstreamAddr: echo.LocalAddr().String(),
	})
	if err != nil {
		t.Fatalf("new relay: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = relay.Close()
	})
	go func() { _ = relay.Run(ctx) }()

	client, err := net.DialUDP("udp", nil, relay.Addr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("dial relay: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if err := client.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}

	for _, msg := range []string{"ping", "hello", "ещё раз"} {
		if _, err := client.Write([]byte(msg)); err != nil {
			t.Fatalf("write %q: %v", msg, err)
		}

		buf := make([]byte, 1024)
		n, err := client.Read(buf)
		if err != nil {
			t.Fatalf("read reply to %q: %v", msg, err)
		}

		want := "echo:" + msg
		if got := string(buf[:n]); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
}

func TestRelaySeparatesClients(t *testing.T) {
	// The upstream reports the source address it sees, so we can verify that
	// each client arrives through its own upstream socket.
	upstream, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen upstream: %v", err)
	}
	t.Cleanup(func() { _ = upstream.Close() })

	go func() {
		buf := make([]byte, 1024)
		for {
			n, addr, err := upstream.ReadFromUDP(buf)
			if err != nil {
				return
			}
			_, _ = upstream.WriteToUDP([]byte(addr.String()+" "+string(buf[:n])), addr)
		}
	}()

	relay, err := New(Config{
		ListenAddr:   "127.0.0.1:0",
		UpstreamAddr: upstream.LocalAddr().String(),
	})
	if err != nil {
		t.Fatalf("new relay: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = relay.Close()
	})
	go func() { _ = relay.Run(ctx) }()

	const clients = 3
	var wg sync.WaitGroup
	seen := make([]string, clients)

	for i := range clients {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			conn, err := net.DialUDP("udp", nil, relay.Addr().(*net.UDPAddr))
			if err != nil {
				t.Errorf("dial relay: %v", err)
				return
			}
			defer conn.Close()

			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			if _, err := conn.Write([]byte(fmt.Sprintf("client-%d", i))); err != nil {
				t.Errorf("write: %v", err)
				return
			}

			buf := make([]byte, 1024)
			n, err := conn.Read(buf)
			if err != nil {
				t.Errorf("read: %v", err)
				return
			}
			seen[i] = string(buf[:n])
		}(i)
	}
	wg.Wait()

	unique := make(map[string]struct{})
	for i, got := range seen {
		if got == "" {
			t.Fatalf("client %d got no reply", i)
		}
		addr, _, ok := strings.Cut(got, " ")
		if !ok {
			t.Fatalf("client %d: unexpected reply %q", i, got)
		}
		unique[addr] = struct{}{}
	}

	if len(unique) != clients {
		t.Fatalf("upstream saw %d distinct source addresses, want %d: %v", len(unique), clients, unique)
	}
}

func TestRunStopsOnContextCancel(t *testing.T) {
	echo := echoServer(t)

	relay, err := New(Config{
		ListenAddr:   "127.0.0.1:0",
		UpstreamAddr: echo.LocalAddr().String(),
	})
	if err != nil {
		t.Fatalf("new relay: %v", err)
	}
	t.Cleanup(func() { _ = relay.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after the context was cancelled")
	}
}

package delivery

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/kirill-scherba/sqt"
	"github.com/kirill-scherba/sqt/internal/devtls"
)

// chaosServer starts a sqt server with a delivery server attached and returns
// a function that drops every connected peer.
func chaosServer(t *testing.T, network string) (*sqt.Server, *Server, func()) {
	t.Helper()

	cert, _, err := devtls.SelfSigned()
	if err != nil {
		t.Fatalf("self-signed certificate: %v", err)
	}

	srv, err := sqt.NewServer(sqt.ServerConfig{
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

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(func() {
		cancel()
		_ = srv.Close()
	})

	ds := NewServer()

	go func() { _ = srv.Run(ctx) }()
	go func() {
		for p := range srv.Peers() {
			go ds.Handle(ctx, p)
		}
	}()

	drop := func() {
		for _, id := range srv.PeerIDs() {
			if p, ok := srv.Peer(id); ok {
				_ = p.Close()
			}
		}
	}

	return srv, ds, drop
}

func TestDeliverySurvivesChaos(t *testing.T) {
	for _, network := range []string{sqt.NetworkQUIC, sqt.NetworkTCP} {
		t.Run(network, func(t *testing.T) {
			srv, ds, drop := chaosServer(t, network)

			const clientName = "chaos-client"
			const commands = 10

			var mu sync.Mutex
			delivered := make(map[uint64]bool)
			done := make(chan struct{}, commands)

			client := sqt.NewClient(sqt.ClientConfig{
				Addr:    srv.Addr().String(),
				Network: network,
				TLSConfig: &tls.Config{
					InsecureSkipVerify: true,
					MinVersion:         tls.VersionTLS13,
				},
			})

			dc := NewClient(clientName, func(id uint64, text string) {
				mu.Lock()
				if delivered[id] {
					mu.Unlock()
					t.Errorf("command %d delivered twice", id)
					return
				}
				delivered[id] = true
				mu.Unlock()
				done <- struct{}{}
			})

			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
			defer cancel()

			go func() {
				_ = client.Run(ctx, func(ctx context.Context, p *sqt.Peer) {
					dc.Handle(ctx, p)
				})
			}()

			// Tear the connection down every 250ms.
			go func() {
				ticker := time.NewTicker(250 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-ticker.C:
						drop()
					case <-ctx.Done():
						return
					}
				}
			}()

			// Wait for the first connection, then enqueue commands one by one
			// while the path keeps breaking. Some commands land during an
			// outage, some acks are lost and cause a resend.
			time.Sleep(300 * time.Millisecond)

			ids := make([]uint64, 0, commands)
			for i := range commands {
				ids = append(ids, ds.Enqueue(clientName, fmt.Sprintf("command-%d", i)))
				time.Sleep(400 * time.Millisecond)
			}

			for range commands {
				select {
				case <-done:
				case <-time.After(40 * time.Second):
					mu.Lock()
					t.Fatalf("delivered %d of %d: %v", len(delivered), commands, delivered)
					mu.Unlock()
				}
			}

			mu.Lock()
			for _, id := range ids {
				if !delivered[id] {
					t.Fatalf("command %d was not delivered", id)
				}
			}
			mu.Unlock()
		})
	}
}

func TestQueueKeepsUnackedCommands(t *testing.T) {
	ds := NewServer()
	id := ds.Enqueue("kitchen", "turn on the light")

	if got := ds.Pending("kitchen"); got != 1 {
		t.Fatalf("pending = %d, want 1", got)
	}
	if id == 0 {
		t.Fatal("command id must not be zero")
	}

	// Without a client the command stays queued; a second enqueue gets a new id.
	second := ds.Enqueue("kitchen", "turn off the light")
	if second == id {
		t.Fatal("command ids must be unique")
	}
	if got := ds.Pending("kitchen"); got != 2 {
		t.Fatalf("pending = %d, want 2", got)
	}
}

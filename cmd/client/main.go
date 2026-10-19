// Command client is a test application built on the sqt package. It connects
// to the server, reconnects automatically, identifies itself by name and
// executes commands exactly once, acknowledging them to the server.
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kirill-scherba/sqt"
	"github.com/kirill-scherba/sqt/internal/delivery"
)

func main() {
	addr := flag.String("addr", "localhost:4433", "server address (host:port)")
	network := flag.String("network", "quic", `transport: "quic" or "tcp"`)
	name := flag.String("name", defaultName(), "stable client name for command delivery")
	serverName := flag.String("servername", "", "TLS SNI name when dialing an IP address")
	insecure := flag.Bool("insecure", false, "skip TLS certificate verification")
	keepAlive := flag.Duration("keepalive", 0, "QUIC keepalive period (0 = 15s)")
	idle := flag.Duration("idle", 0, "QUIC idle timeout (0 = 60s)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tlsConf := &tls.Config{
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: *insecure,
	}
	if *serverName != "" {
		tlsConf.ServerName = *serverName
	}

	client := sqt.NewClient(sqt.ClientConfig{
		Addr:            *addr,
		Network:         *network,
		TLSConfig:       tlsConf,
		KeepAlivePeriod: *keepAlive,
		IdleTimeout:     *idle,
	})

	client.OnRetry = func(err error, delay time.Duration) {
		if err != nil {
			log.Printf("connection lost: %v; reconnecting in %s", err, delay.Round(time.Millisecond))
			return
		}
		log.Printf("reconnecting in %s", delay.Round(time.Millisecond))
	}

	deliver := delivery.NewClient(*name, func(id uint64, text string) {
		// This is where a real application performs the command. The library
		// sends the ack only after the handler returns, so a crash before the
		// ack causes a redelivery after reconnect.
		log.Printf("command #%d: %s", id, text)
	})

	input := make(chan string, 16)
	go readInput(ctx, stop, input)

	err := client.Run(ctx, func(ctx context.Context, peer *sqt.Peer) {
		connectedAt := time.Now()
		log.Printf("connected to %s over %s as %s", *addr, *network, *name)

		done := make(chan struct{})
		go func() {
			deliver.Handle(ctx, peer)
			close(done)
		}()

		for {
			select {
			case <-ctx.Done():
				return

			case <-done:
				reportClosed(peer, connectedAt)
				return

			case line, ok := <-input:
				if !ok {
					return
				}
				if !send(ctx, peer, line) {
					return
				}
				log.Printf("sent: %s", line)
			}
		}
	})
	if err != nil {
		log.Fatalf("client: %v", err)
	}
}

func defaultName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "client"
	}
	return host
}

func send(ctx context.Context, peer *sqt.Peer, text string) bool {
	sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := peer.Send(sendCtx, []byte(text)); err != nil {
		log.Printf("send: %v", err)
		return false
	}
	return true
}

// readInput reads stdin once, outside the reconnect loop, so lines typed
// while the connection is down stay queued in the channel.
func readInput(ctx context.Context, stop context.CancelFunc, out chan<- string) {
	defer close(out)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == "quit" || line == "exit" {
			stop()
			return
		}
		select {
		case out <- line:
		case <-ctx.Done():
			return
		}
	}
}

// reportClosed prints why the connection ended.
func reportClosed(peer *sqt.Peer, connectedAt time.Time) {
	elapsed := time.Since(connectedAt).Round(time.Second)
	if err := peer.Err(); err != nil {
		log.Printf("disconnected after %s: %v", elapsed, err)
		return
	}
	log.Printf("disconnected after %s", elapsed)
}

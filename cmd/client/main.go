// Command client is a test application built on the sqt package. It dials the
// server, prints every received message and sends a reply back.
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
)

func main() {
	addr := flag.String("addr", "localhost:4433", "server address (host:port)")
	insecure := flag.Bool("insecure", false, "skip TLS certificate verification")
	reply := flag.String("reply", "", `reply text; empty means "reply: <message>"`)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tlsConf := &tls.Config{
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: *insecure,
	}

	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	peer, err := sqt.Dial(dialCtx, sqt.ClientConfig{Addr: *addr, TLSConfig: tlsConf})
	cancel()
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer peer.Close()

	log.Printf("connected to %s (keepalive %s, idle timeout %s)",
		*addr, sqt.DefaultKeepAlivePeriod, sqt.DefaultIdleTimeout)
	connectedAt := time.Now()

	go func() {
		<-ctx.Done()
		_ = peer.Close()
	}()

	go console(ctx, peer)

	for msg := range peer.Messages() {
		log.Printf("received: %s", msg)

		out := *reply
		if out == "" {
			out = "reply: " + string(msg)
		}

		sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := peer.Send(sendCtx, []byte(out))
		cancel()
		if err != nil {
			log.Printf("send reply: %v", err)
			continue
		}
		log.Printf("reply sent: %s", out)
	}

	reportClosed(peer, connectedAt)
}

// reportClosed prints why the connection ended. The reason matters: an idle
// timeout means the path went silent, a transport error means a network reset,
// and a remote application close means the server shut the connection down.
func reportClosed(peer *sqt.Peer, connectedAt time.Time) {
	elapsed := time.Since(connectedAt).Round(time.Second)
	if err := peer.Err(); err != nil {
		log.Printf("disconnected after %s: %v", elapsed, err)
		return
	}
	log.Printf("disconnected after %s", elapsed)
}

// console reads lines from stdin and sends each one to the server. Because
// sqt is symmetric, the client can push messages at any time, not only in
// response to an incoming message.
func console(ctx context.Context, peer *sqt.Peer) {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == "quit" || line == "exit" {
			_ = peer.Close()
			return
		}

		sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := peer.Send(sendCtx, []byte(line))
		cancel()
		if err != nil {
			log.Printf("send: %v", err)
			continue
		}
		log.Printf("sent: %s", line)
	}
}

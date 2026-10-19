// Command relay forwards UDP datagrams between clients and a single upstream
// server without touching the kernel firewall. It is useful when the server
// itself is unreachable from the client's network, but an intermediate host
// can reach both sides.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kirill-scherba/sqt/internal/udprelay"
)

func main() {
	listen := flag.String("listen", ":4433", "UDP address to listen on")
	upstream := flag.String("upstream", "", "UDP address of the real server")
	idle := flag.Duration("idle", 2*time.Minute, "idle session timeout")
	verbose := flag.Bool("v", false, "log session open and close")
	flag.Parse()

	if *upstream == "" {
		log.Fatal("-upstream is required")
	}

	var logf func(string, ...any)
	if *verbose {
		logf = log.Printf
	}

	relay, err := udprelay.New(udprelay.Config{
		ListenAddr:   *listen,
		UpstreamAddr: *upstream,
		IdleTimeout:  *idle,
		Logf:         logf,
	})
	if err != nil {
		log.Fatalf("relay: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("listening on udp %s, forwarding to %s", relay.Addr(), *upstream)

	if err := relay.Run(ctx); err != nil {
		log.Printf("relay stopped: %v", err)
	}
	_ = relay.Close()
}

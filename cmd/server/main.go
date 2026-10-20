// Command server is a test application built on the sqt package. It listens
// for clients, keeps a delivery queue per client name and lets the operator
// send a command with "<name>, <text>". Commands survive disconnects.
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/kirill-scherba/sqt"
	"github.com/kirill-scherba/sqt/delivery"
	"github.com/kirill-scherba/sqt/internal/devtls"
	"golang.org/x/term"
)

func main() {
	addr := flag.String("addr", ":4433", "address to listen on")
	network := flag.String("network", "quic", `transport: "quic" or "tcp"`)
	certFile := flag.String("cert", "", "TLS certificate file (PEM)")
	keyFile := flag.String("key", "", "TLS key file (PEM)")
	consoleMode := flag.String("console", "auto", `console mode: "auto" (only with a terminal), "on", "off"`)
	keepAlive := flag.Duration("keepalive", 0, "QUIC keepalive period (0 = 15s)")
	idle := flag.Duration("idle", 0, "QUIC idle timeout (0 = 60s)")
	flag.Parse()

	tlsConf, err := tlsConfig(*certFile, *keyFile)
	if err != nil {
		log.Fatalf("tls: %v", err)
	}

	srv, err := sqt.NewServer(sqt.ServerConfig{
		Addr:            *addr,
		Network:         *network,
		TLSConfig:       tlsConf,
		KeepAlivePeriod: *keepAlive,
		IdleTimeout:     *idle,
	})
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	queue := delivery.NewServer()
	queue.OnData = func(name, text string) {
		log.Printf("data from %s: %s", name, text)
	}

	go queue.Run(ctx)

	log.Printf("listening on %s %s", srv.Network(), srv.Addr())

	go func() {
		if err := srv.Run(ctx); err != nil {
			log.Printf("server stopped: %v", err)
			stop()
		}
	}()

	go func() {
		for p := range srv.Peers() {
			log.Printf("client %s connected (%s)", p.ID(), p.RemoteAddr())
			go logDisconnect(p)
			go queue.Handle(ctx, p)
		}
	}()

	if useConsole(*consoleMode) {
		consoleDone := make(chan struct{})
		go func() {
			console(queue)
			close(consoleDone)
		}()

		select {
		case <-consoleDone:
		case <-ctx.Done():
		}
	} else {
		log.Printf("stdin is not a terminal, running headless (use -console on to force the console)")
		<-ctx.Done()
	}

	_ = srv.Close()
}

func tlsConfig(certFile, keyFile string) (*tls.Config, error) {
	if certFile != "" || keyFile != "" {
		if certFile == "" || keyFile == "" {
			return nil, fmt.Errorf("both -cert and -key are required")
		}
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		return &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS13,
		}, nil
	}

	cert, fingerprint, err := devtls.SelfSigned()
	if err != nil {
		return nil, err
	}
	log.Printf("no certificate supplied, using a generated self-signed one")
	log.Printf("sha256 fingerprint: %s", fingerprint)
	log.Printf("start the client with -insecure to skip verification")
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	}, nil
}

func useConsole(mode string) bool {
	switch mode {
	case "on":
		return true
	case "off":
		return false
	}

	fi, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	if fi.Mode()&os.ModeCharDevice == 0 {
		return false
	}
	// /dev/null (and NUL on Windows) is a character device too, so ask the
	// terminal layer whether stdin is really attached to a terminal.
	return term.IsTerminal(int(os.Stdin.Fd()))
}

func logDisconnect(p *sqt.Peer) {
	<-p.Done()

	if err := p.Err(); err != nil {
		log.Printf("client %s disconnected: %v", p.ID(), err)
		return
	}
	log.Printf("client %s disconnected", p.ID())
}

func console(queue *delivery.Server) {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			return
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		switch line {
		case "quit", "exit":
			return
		case "ls", "list":
			names := queue.Names()
			if len(names) == 0 {
				fmt.Println("no clients connected")
				continue
			}
			fmt.Printf("clients: %s\n", strings.Join(names, ", "))
			continue
		}

		name, text, ok := strings.Cut(line, ",")
		if !ok {
			fmt.Println("usage: <client-name>, <text>")
			continue
		}
		name = strings.TrimSpace(name)
		text = strings.TrimSpace(text)

		id := queue.Enqueue(name, text)
		fmt.Printf("queued #%d for %s\n", id, name)
	}
}

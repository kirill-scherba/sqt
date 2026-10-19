// Command server is a test application built on the sqt package. It listens
// for QUIC clients, prints connection events and lets the operator send a
// message to a connected client with "<id>, <text>".
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
	"time"

	"github.com/kirill-scherba/sqt"
	"github.com/kirill-scherba/sqt/internal/devtls"
)

func main() {
	addr := flag.String("addr", ":4433", "UDP address to listen on")
	certFile := flag.String("cert", "", "TLS certificate file (PEM)")
	keyFile := flag.String("key", "", "TLS key file (PEM)")
	flag.Parse()

	tlsConf, err := tlsConfig(*certFile, *keyFile)
	if err != nil {
		log.Fatalf("tls: %v", err)
	}

	srv, err := sqt.NewServer(sqt.ServerConfig{Addr: *addr, TLSConfig: tlsConf})
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("listening on udp %s", srv.Addr())

	go func() {
		if err := srv.Run(ctx); err != nil {
			log.Printf("server stopped: %v", err)
			stop()
		}
	}()

	go func() {
		for p := range srv.Peers() {
			log.Printf("client %s connected (%s)", p.ID(), p.RemoteAddr())
			go watchPeer(p)
		}
	}()

	consoleDone := make(chan struct{})
	go func() {
		console(ctx, srv)
		close(consoleDone)
	}()

	// The console blocks on stdin, so wait for either it or the signal
	// context. Returning from main terminates the process and unblocks the
	// stdin reader.
	select {
	case <-consoleDone:
	case <-ctx.Done():
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

func watchPeer(p *sqt.Peer) {
	go func() {
		<-p.Done()
		log.Printf("client %s disconnected", p.ID())
	}()

	for msg := range p.Messages() {
		log.Printf("received from %s: %s", p.ID(), msg)
	}
}

func console(ctx context.Context, srv *sqt.Server) {
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
			ids := srv.PeerIDs()
			if len(ids) == 0 {
				fmt.Println("no clients connected")
				continue
			}
			fmt.Printf("clients: %s\n", strings.Join(ids, ", "))
			continue
		}

		id, text, ok := strings.Cut(line, ",")
		if !ok {
			fmt.Println("usage: <id>, <text>")
			continue
		}
		id = strings.TrimSpace(id)
		text = strings.TrimSpace(text)

		peer, ok := srv.Peer(id)
		if !ok {
			fmt.Printf("client %s is not connected\n", id)
			continue
		}

		sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := peer.Send(sendCtx, []byte(text))
		cancel()
		if err != nil {
			fmt.Printf("send to %s: %v\n", id, err)
			continue
		}
		fmt.Printf("sent to %s: %s\n", id, text)
	}
}

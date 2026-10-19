package sqt

import (
	"context"
	"crypto/tls"
	"net"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
)

// ServerConfig configures a Server.
type ServerConfig struct {
	// Addr is the UDP address to listen on, for example ":4433".
	Addr string
	// TLSConfig must contain at least one certificate. NextProtos is set by
	// the package.
	TLSConfig *tls.Config
	// KeepAlivePeriod is how often a PING is sent when the connection is
	// idle. Zero means DefaultKeepAlivePeriod.
	KeepAlivePeriod time.Duration
	// IdleTimeout is offered to the peer; the effective value is the minimum
	// of both sides. Zero means DefaultIdleTimeout.
	IdleTimeout time.Duration
}

// Server accepts QUIC connections and keeps track of connected peers.
type Server struct {
	ln *quic.Listener

	peers  chan *Peer
	closed chan struct{}

	mu     sync.RWMutex
	byID   map[string]*Peer
	nextID atomic.Uint64

	closeOnce sync.Once
}

// NewServer starts listening on cfg.Addr.
func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.TLSConfig == nil {
		return nil, ErrTLSConfigRequired
	}

	tlsConf := cfg.TLSConfig.Clone()
	tlsConf.NextProtos = []string{ALPN}

	ln, err := quic.ListenAddr(cfg.Addr, tlsConf, quicConfig(cfg.KeepAlivePeriod, cfg.IdleTimeout))
	if err != nil {
		return nil, err
	}

	return &Server{
		ln:     ln,
		peers:  make(chan *Peer, defaultPeersBuffer),
		closed: make(chan struct{}),
		byID:   make(map[string]*Peer),
	}, nil
}

// Addr returns the address the server is listening on. When the configured
// port is zero, it returns the actual port chosen by the system.
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Peers returns a channel of newly connected peers. Peers that are already
// connected when the channel is read are not replayed.
func (s *Server) Peers() <-chan *Peer { return s.peers }

// Peer returns the connected peer with the given identifier.
func (s *Server) Peer(id string) (*Peer, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.byID[id]
	return p, ok
}

// PeerIDs returns the identifiers of all connected peers in ascending order.
func (s *Server) PeerIDs() []string {
	s.mu.RLock()
	ids := make([]string, 0, len(s.byID))
	for id := range s.byID {
		ids = append(ids, id)
	}
	s.mu.RUnlock()

	sort.Slice(ids, func(i, j int) bool {
		a, _ := strconv.Atoi(ids[i])
		b, _ := strconv.Atoi(ids[j])
		return a < b
	})
	return ids
}

// Run accepts connections until ctx is cancelled or the server is closed.
func (s *Server) Run(ctx context.Context) error {
	for {
		conn, err := s.ln.Accept(ctx)
		if err != nil {
			select {
			case <-s.closed:
				return nil
			case <-ctx.Done():
				return nil
			default:
				return err
			}
		}

		p := s.register(conn)

		select {
		case s.peers <- p:
		case <-ctx.Done():
			_ = p.Close()
			return nil
		case <-s.closed:
			_ = p.Close()
			return nil
		}
	}
}

// Close stops the listener and closes all active connections.
func (s *Server) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.closed)
		err = s.ln.Close()
		s.mu.RLock()
		peers := make([]*Peer, 0, len(s.byID))
		for _, p := range s.byID {
			peers = append(peers, p)
		}
		s.mu.RUnlock()
		for _, p := range peers {
			_ = p.Close()
		}
	})
	return err
}

func (s *Server) register(conn *quic.Conn) *Peer {
	id := strconv.FormatUint(s.nextID.Add(1), 10)
	p := newPeer(id, conn)

	s.mu.Lock()
	s.byID[id] = p
	s.mu.Unlock()

	go func() {
		<-p.Done()
		s.mu.Lock()
		delete(s.byID, id)
		s.mu.Unlock()
	}()

	return p
}

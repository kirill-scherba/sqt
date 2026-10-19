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
	// Addr is the address to listen on, for example ":4433".
	Addr string
	// Network selects the transport: NetworkQUIC (default) or NetworkTCP.
	Network string
	// TLSConfig must contain at least one certificate. NextProtos is set by
	// the package.
	TLSConfig *tls.Config
	// KeepAlivePeriod applies to QUIC only. Zero means DefaultKeepAlivePeriod.
	KeepAlivePeriod time.Duration
	// IdleTimeout applies to QUIC only. Zero means DefaultIdleTimeout.
	IdleTimeout time.Duration
}

// Server accepts connections and keeps track of connected peers.
type Server struct {
	network string
	ln      transportListener

	peers  chan *Peer
	closed chan struct{}

	mu     sync.RWMutex
	byID   map[string]*Peer
	nextID atomic.Uint64

	closeOnce sync.Once
}

// NewServer starts listening on cfg.Addr.
func NewServer(cfg ServerConfig) (*Server, error) {
	network := cfg.Network
	if network == "" {
		network = NetworkQUIC
	}
	if network != NetworkQUIC && network != NetworkTCP {
		return nil, ErrUnknownNetwork
	}

	if cfg.TLSConfig == nil {
		return nil, ErrTLSConfigRequired
	}

	tlsConf := cfg.TLSConfig.Clone()
	tlsConf.NextProtos = []string{ALPN}

	var ln transportListener
	switch network {
	case NetworkQUIC:
		quicLn, err := quic.ListenAddr(cfg.Addr, tlsConf, quicConfig(cfg.KeepAlivePeriod, cfg.IdleTimeout))
		if err != nil {
			return nil, err
		}
		ln = &quicListener{ln: quicLn}
	case NetworkTCP:
		tcpLn, err := net.Listen("tcp", cfg.Addr)
		if err != nil {
			return nil, err
		}
		ln = &tcpListener{ln: tcpLn, tlsConf: tlsConf}
	default:
		return nil, ErrUnknownNetwork
	}

	return &Server{
		network: network,
		ln:      ln,
		peers:   make(chan *Peer, defaultPeersBuffer),
		closed:  make(chan struct{}),
		byID:    make(map[string]*Peer),
	}, nil
}

// Network returns the transport the server listens on.
func (s *Server) Network() string { return s.network }

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
	// Neither QUIC's Accept nor TCP's Accept is interrupted by a context, so
	// closing the listener is what unblocks them.
	go func() {
		select {
		case <-ctx.Done():
			_ = s.ln.Close()
		case <-s.closed:
		}
	}()

	for {
		tr, err := s.ln.Accept(ctx)
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

		p := s.register(tr)

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

func (s *Server) register(tr transport) *Peer {
	id := strconv.FormatUint(s.nextID.Add(1), 10)
	p := newPeer(id, tr)

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

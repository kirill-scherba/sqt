// Package udprelay forwards UDP datagrams between clients and a single
// upstream server. It is transport-agnostic: QUIC packets pass through it
// untouched, because QUIC demultiplexes connections by connection ID rather
// than by the UDP 4-tuple.
//
// Every client gets its own upstream socket. Without that, all clients would
// share one source address at the server and the QUIC path handling would see
// a single, constantly moving endpoint.
package udprelay

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Config configures a Relay.
type Config struct {
	// ListenAddr is the UDP address clients send to, for example ":4433".
	ListenAddr string
	// UpstreamAddr is the UDP address of the real server.
	UpstreamAddr string
	// IdleTimeout closes sessions that have seen no traffic for this long.
	// Zero means two minutes.
	IdleTimeout time.Duration
	// Logf receives session lifecycle messages. Nil disables logging.
	Logf func(format string, args ...any)
}

// Relay forwards datagrams between clients and the upstream server.
type Relay struct {
	ln       *net.UDPConn
	upstream *net.UDPAddr
	idle     time.Duration
	logf     func(format string, args ...any)

	mu       sync.Mutex
	sessions map[string]*session

	closed    chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

type session struct {
	key      string
	client   *net.UDPAddr
	upstream *net.UDPConn
	relay    *Relay

	lastSeen  atomic.Int64 // unix nanoseconds
	closeOnce sync.Once
}

// New binds the listening socket and resolves the upstream address.
func New(cfg Config) (*Relay, error) {
	if cfg.UpstreamAddr == "" {
		return nil, errors.New("udprelay: upstream address is required")
	}

	upstream, err := net.ResolveUDPAddr("udp", cfg.UpstreamAddr)
	if err != nil {
		return nil, err
	}

	var listen *net.UDPAddr
	if cfg.ListenAddr != "" {
		listen, err = net.ResolveUDPAddr("udp", cfg.ListenAddr)
		if err != nil {
			return nil, err
		}
	}

	ln, err := net.ListenUDP("udp", listen)
	if err != nil {
		return nil, err
	}

	idle := cfg.IdleTimeout
	if idle == 0 {
		idle = 2 * time.Minute
	}

	logf := cfg.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}

	return &Relay{
		ln:       ln,
		upstream: upstream,
		idle:     idle,
		logf:     logf,
		sessions: make(map[string]*session),
		closed:   make(chan struct{}),
	}, nil
}

// Addr returns the address the relay listens on.
func (r *Relay) Addr() net.Addr { return r.ln.LocalAddr() }

// Run forwards datagrams until ctx is cancelled or the relay is closed.
func (r *Relay) Run(ctx context.Context) error {
	go r.cleanup(ctx)

	// ReadFromUDP has no context, so closing the listener is what unblocks it.
	go func() {
		select {
		case <-ctx.Done():
			_ = r.ln.Close()
		case <-r.closed:
		}
	}()

	buf := make([]byte, 64*1024)
	for {
		n, client, err := r.ln.ReadFromUDP(buf)
		if err != nil {
			if r.isClosed() || ctx.Err() != nil {
				return nil
			}
			return err
		}

		s := r.session(client)
		if s == nil {
			continue
		}
		s.touch()

		if _, err := s.upstream.Write(buf[:n]); err != nil {
			s.close()
		}
	}
}

// Close stops the relay and closes every session.
func (r *Relay) Close() error {
	var err error
	r.closeOnce.Do(func() {
		close(r.closed)
		err = r.ln.Close()

		r.mu.Lock()
		sessions := make([]*session, 0, len(r.sessions))
		for _, s := range r.sessions {
			sessions = append(sessions, s)
		}
		r.mu.Unlock()

		for _, s := range sessions {
			s.close()
		}
		r.wg.Wait()
	})
	return err
}

func (r *Relay) isClosed() bool {
	select {
	case <-r.closed:
		return true
	default:
		return false
	}
}

func (r *Relay) session(client *net.UDPAddr) *session {
	key := client.String()

	r.mu.Lock()
	s := r.sessions[key]
	r.mu.Unlock()
	if s != nil {
		return s
	}

	conn, err := net.DialUDP("udp", nil, r.upstream)
	if err != nil {
		r.logf("session %s: %v", key, err)
		return nil
	}

	s = &session{key: key, client: client, upstream: conn, relay: r}
	s.touch()

	r.mu.Lock()
	if existing := r.sessions[key]; existing != nil {
		r.mu.Unlock()
		_ = conn.Close()
		return existing
	}
	r.sessions[key] = s
	r.mu.Unlock()

	r.logf("session %s opened", key)

	r.wg.Add(1)
	go s.readUpstream()

	return s
}

func (r *Relay) remove(s *session) {
	r.mu.Lock()
	if r.sessions[s.key] == s {
		delete(r.sessions, s.key)
	}
	r.mu.Unlock()
	r.logf("session %s closed", s.key)
}

func (r *Relay) cleanup(ctx context.Context) {
	interval := r.idle / 2
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.sweep()
		case <-ctx.Done():
			return
		case <-r.closed:
			return
		}
	}
}

func (r *Relay) sweep() {
	now := time.Now()

	r.mu.Lock()
	var stale []*session
	for _, s := range r.sessions {
		if now.Sub(s.last()) > r.idle {
			stale = append(stale, s)
		}
	}
	r.mu.Unlock()

	for _, s := range stale {
		s.close()
	}
}

func (s *session) readUpstream() {
	defer s.relay.wg.Done()

	buf := make([]byte, 64*1024)
	for {
		n, err := s.upstream.Read(buf)
		if err != nil {
			s.close()
			return
		}
		s.touch()

		if _, err := s.relay.ln.WriteToUDP(buf[:n], s.client); err != nil {
			s.close()
			return
		}
	}
}

func (s *session) touch() {
	s.lastSeen.Store(time.Now().UnixNano())
}

func (s *session) last() time.Time {
	return time.Unix(0, s.lastSeen.Load())
}

func (s *session) close() {
	s.closeOnce.Do(func() {
		_ = s.upstream.Close()
		s.relay.remove(s)
	})
}

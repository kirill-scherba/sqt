package sqt

import (
	"context"
	"crypto/tls"
	"math/rand"
	"net"
	"time"

	"github.com/quic-go/quic-go"
)

// ClientConfig configures a dialing peer.
type ClientConfig struct {
	// Addr is the server address in host:port form.
	Addr string
	// Network selects the transport: NetworkQUIC (default) or NetworkTCP.
	Network string
	// TLSConfig is used for the handshake. When nil, the system roots are
	// used. NextProtos is set by the package.
	TLSConfig *tls.Config
	// KeepAlivePeriod applies to QUIC only. Zero means DefaultKeepAlivePeriod.
	KeepAlivePeriod time.Duration
	// IdleTimeout applies to QUIC only. Zero means DefaultIdleTimeout.
	IdleTimeout time.Duration
}

// Dial establishes a connection to the server.
func Dial(ctx context.Context, cfg ClientConfig) (*Peer, error) {
	if cfg.Addr == "" {
		return nil, ErrAddressRequired
	}

	network := cfg.Network
	if network == "" {
		network = NetworkQUIC
	}

	tlsConf := &tls.Config{}
	if cfg.TLSConfig != nil {
		tlsConf = cfg.TLSConfig.Clone()
	}
	if len(tlsConf.NextProtos) == 0 {
		tlsConf.NextProtos = []string{ALPN}
	}

	switch network {
	case NetworkQUIC:
		conn, err := quic.DialAddr(ctx, cfg.Addr, tlsConf, quicConfig(cfg.KeepAlivePeriod, cfg.IdleTimeout))
		if err != nil {
			return nil, err
		}
		return newPeer("", &quicTransport{conn: conn}), nil

	case NetworkTCP:
		// tls.Dialer sets ServerName from the address when it is empty.
		dialer := &tls.Dialer{NetDialer: &net.Dialer{}, Config: tlsConf}
		conn, err := dialer.DialContext(ctx, "tcp", cfg.Addr)
		if err != nil {
			return nil, err
		}
		return newPeer("", newTCPTransport(conn)), nil
	}

	return nil, ErrUnknownNetwork
}

// Client keeps a connection alive and reconnects when it drops. It is a thin
// loop above Dial: the transport and the message API stay the same.
type Client struct {
	Config ClientConfig
	// OnRetry is called before every reconnect attempt, including the first
	// failed dial. It may be nil.
	OnRetry func(err error, delay time.Duration)
}

// NewClient returns a client for the given configuration.
func NewClient(cfg ClientConfig) *Client { return &Client{Config: cfg} }

// Run dials the server, calls handler for every established connection and
// reconnects with exponential backoff until ctx is cancelled. The handler
// must return when the peer is closed or ctx is done; Run then reconnects.
func (c *Client) Run(ctx context.Context, handler func(context.Context, *Peer)) error {
	const (
		minDelay    = time.Second
		maxDelay    = 30 * time.Second
		stableAfter = time.Minute
	)

	delay := minDelay

	for {
		dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		peer, err := Dial(dialCtx, c.Config)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			wait := jitter(delay)
			if c.OnRetry != nil {
				c.OnRetry(err, wait)
			}
			if !sleepCtx(ctx, wait) {
				return nil
			}
			delay = nextDelay(delay, maxDelay)
			continue
		}

		started := time.Now()
		handler(ctx, peer)

		// Capture the reason before Close, which marks the shutdown as local
		// and clears Err.
		peerErr := peer.Err()
		_ = peer.Close()

		if ctx.Err() != nil {
			return nil
		}

		if time.Since(started) >= stableAfter {
			delay = minDelay
		}

		wait := jitter(delay)
		if c.OnRetry != nil {
			c.OnRetry(peerErr, wait)
		}
		if !sleepCtx(ctx, wait) {
			return nil
		}
		delay = nextDelay(delay, maxDelay)
	}
}

func nextDelay(d, max time.Duration) time.Duration {
	d *= 2
	if d > max {
		return max
	}
	return d
}

// jitter spreads reconnect attempts by up to 20% in either direction.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	delta := time.Duration(rand.Int63n(int64(d)/5 + 1))
	if rand.Intn(2) == 0 {
		return d + delta
	}
	return d - delta
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

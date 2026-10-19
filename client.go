package sqt

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/quic-go/quic-go"
)

// ClientConfig configures a dialing peer.
type ClientConfig struct {
	// Addr is the server address in host:port form.
	Addr string
	// TLSConfig is used for the handshake. When nil, the system roots are
	// used. NextProtos is set by the package.
	TLSConfig *tls.Config
	// KeepAlivePeriod is how often a PING is sent when the connection is
	// idle. Zero means DefaultKeepAlivePeriod.
	KeepAlivePeriod time.Duration
	// IdleTimeout is offered to the peer; the effective value is the minimum
	// of both sides. Zero means DefaultIdleTimeout.
	IdleTimeout time.Duration
}

// Dial establishes a QUIC connection to the server.
func Dial(ctx context.Context, cfg ClientConfig) (*Peer, error) {
	if cfg.Addr == "" {
		return nil, ErrAddressRequired
	}

	tlsConf := &tls.Config{}
	if cfg.TLSConfig != nil {
		tlsConf = cfg.TLSConfig.Clone()
	}
	if len(tlsConf.NextProtos) == 0 {
		tlsConf.NextProtos = []string{ALPN}
	}

	conn, err := quic.DialAddr(ctx, cfg.Addr, tlsConf, quicConfig(cfg.KeepAlivePeriod, cfg.IdleTimeout))
	if err != nil {
		return nil, err
	}

	return newPeer("", conn), nil
}

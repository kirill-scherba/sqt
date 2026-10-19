package sqt

import (
	"context"
	"crypto/tls"
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

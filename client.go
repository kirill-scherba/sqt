package sqt

import (
	"context"
	"crypto/tls"

	"github.com/quic-go/quic-go"
)

// ClientConfig configures a dialing peer.
type ClientConfig struct {
	// Addr is the server address in host:port form.
	Addr string
	// TLSConfig is used for the handshake. When nil, the system roots are
	// used. NextProtos is set by the package.
	TLSConfig *tls.Config
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

	conn, err := quic.DialAddr(ctx, cfg.Addr, tlsConf, defaultQUICConfig())
	if err != nil {
		return nil, err
	}

	return newPeer("", conn), nil
}

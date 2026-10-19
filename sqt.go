// Package sqt implements a simple message transport on top of QUIC.
//
// A server listens for QUIC connections and clients dial it. Once a
// connection is established either side can open a stream and send a
// message; each message travels in its own bidirectional stream and is
// terminated by FIN. Messages are opaque byte slices: the package knows
// nothing about their meaning. Addressing, request/response semantics and
// reconnection are left to the layer above.
package sqt

import (
	"errors"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
)

const (
	// ALPN is the application protocol negotiated during the TLS handshake.
	ALPN = "sqt/1"

	// NetworkQUIC carries one message per QUIC stream.
	NetworkQUIC = "quic"
	// NetworkTCP carries length-prefixed messages over one TLS connection.
	NetworkTCP = "tcp"

	// MaxMessageSize is the maximum size of a single message in bytes.
	MaxMessageSize = 1 << 20 // 1 MiB
)

var (
	// ErrTLSConfigRequired is returned when no TLS configuration is supplied.
	ErrTLSConfigRequired = errors.New("sqt: TLS config is required")

	// ErrMessageTooLarge is returned when a message exceeds MaxMessageSize.
	ErrMessageTooLarge = errors.New("sqt: message exceeds MaxMessageSize")

	// ErrPeerClosed is returned when sending on a closed peer.
	ErrPeerClosed = errors.New("sqt: peer is closed")

	// ErrAddressRequired is returned when no dial address is supplied.
	ErrAddressRequired = errors.New("sqt: address is required")

	// ErrUnknownNetwork is returned for an unsupported transport name.
	ErrUnknownNetwork = errors.New("sqt: unknown network")

	// ErrHeartbeatTimeout is returned when the peer stops answering pings.
	ErrHeartbeatTimeout = errors.New("sqt: heartbeat timeout")
)

const (
	// DefaultKeepAlivePeriod is how often a QUIC PING is sent when the
	// connection is otherwise idle. It keeps NAT mappings alive.
	DefaultKeepAlivePeriod = 15 * time.Second

	// DefaultIdleTimeout is the idle timeout offered to the peer. The
	// effective value is the minimum of both endpoints' values.
	DefaultIdleTimeout = 60 * time.Second

	defaultMaxStreams     = 1024
	defaultMessagesBuffer = 16
	defaultPeersBuffer    = 64
)

func quicConfig(keepAlive, idle time.Duration) *quic.Config {
	if keepAlive == 0 {
		keepAlive = DefaultKeepAlivePeriod
	}
	if idle == 0 {
		idle = DefaultIdleTimeout
	}
	return &quic.Config{
		KeepAlivePeriod:    keepAlive,
		MaxIdleTimeout:     idle,
		MaxIncomingStreams: defaultMaxStreams,
		// Writes qlog files when the QLOGDIR environment variable is set and
		// stays inert otherwise.
		Tracer: qlog.DefaultConnectionTracer,
	}
}

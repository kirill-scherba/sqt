package sqt

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"
)

// tcpHandshakeTimeout bounds the TLS handshake so a stalled client cannot
// block the accept loop forever.
const tcpHandshakeTimeout = 10 * time.Second

// tcpTransport carries messages over a single TLS connection. Each message is
// a 4-byte big-endian length followed by the payload. Writes are serialized
// because several goroutines may call Send.
type tcpTransport struct {
	conn net.Conn
	r    *bufio.Reader
	wmu  sync.Mutex
}

func newTCPTransport(conn net.Conn) *tcpTransport {
	return &tcpTransport{conn: conn, r: bufio.NewReader(conn)}
}

func (t *tcpTransport) Send(ctx context.Context, msg []byte) error {
	if len(msg) > MaxMessageSize {
		return ErrMessageTooLarge
	}

	if deadline, ok := ctx.Deadline(); ok {
		_ = t.conn.SetWriteDeadline(deadline)
	} else {
		_ = t.conn.SetWriteDeadline(time.Time{})
	}

	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(msg)))

	t.wmu.Lock()
	defer t.wmu.Unlock()

	if _, err := t.conn.Write(header[:]); err != nil {
		return err
	}
	if _, err := t.conn.Write(msg); err != nil {
		return err
	}
	return nil
}

func (t *tcpTransport) Receive(ctx context.Context) ([]byte, error) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = t.conn.SetReadDeadline(deadline)
	} else {
		_ = t.conn.SetReadDeadline(time.Time{})
	}

	var header [4]byte
	if _, err := io.ReadFull(t.r, header[:]); err != nil {
		return nil, err
	}

	size := binary.BigEndian.Uint32(header[:])
	if size > MaxMessageSize {
		return nil, ErrMessageTooLarge
	}

	buf := make([]byte, size)
	if _, err := io.ReadFull(t.r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func (t *tcpTransport) Close() error { return t.conn.Close() }

func (t *tcpTransport) RemoteAddr() net.Addr { return t.conn.RemoteAddr() }

type tcpListener struct {
	ln      net.Listener
	tlsConf *tls.Config
}

func (l *tcpListener) Accept(ctx context.Context) (transport, error) {
	for {
		conn, err := l.ln.Accept()
		if err != nil {
			return nil, err
		}

		handshakeCtx, cancel := context.WithTimeout(ctx, tcpHandshakeTimeout)
		tlsConn := tls.Server(conn, l.tlsConf)
		err = tlsConn.HandshakeContext(handshakeCtx)
		cancel()
		if err != nil {
			_ = tlsConn.Close()
			if ctx.Err() != nil {
				return nil, err
			}
			continue
		}

		return newTCPTransport(tlsConn), nil
	}
}

func (l *tcpListener) Close() error   { return l.ln.Close() }
func (l *tcpListener) Addr() net.Addr { return l.ln.Addr() }

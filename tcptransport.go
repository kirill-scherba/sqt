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

// tcpTransport carries frames over a single TLS connection:
//
//	[4 bytes big-endian length][1 byte type][payload]
//
// The length covers the type byte and the payload. Writes are serialized
// because several goroutines may call Send.
type tcpTransport struct {
	conn net.Conn
	r    *bufio.Reader
	wmu  sync.Mutex
}

func newTCPTransport(conn net.Conn) *tcpTransport {
	enableKeepAlive(conn)
	return &tcpTransport{conn: conn, r: bufio.NewReader(conn)}
}

// enableKeepAlive asks the kernel to probe an idle TCP connection. It does not
// replace the application heartbeat: some middleboxes only treat payload as
// activity.
func enableKeepAlive(conn net.Conn) {
	if tlsConn, ok := conn.(*tls.Conn); ok {
		conn = tlsConn.NetConn()
	}

	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}

	_ = tcpConn.SetKeepAlive(true)
	_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
}

func (t *tcpTransport) Send(ctx context.Context, typ frameType, payload []byte) error {
	if len(payload) > MaxMessageSize {
		return ErrMessageTooLarge
	}

	if deadline, ok := ctx.Deadline(); ok {
		_ = t.conn.SetWriteDeadline(deadline)
	} else {
		_ = t.conn.SetWriteDeadline(time.Time{})
	}

	var header [5]byte
	binary.BigEndian.PutUint32(header[:4], uint32(len(payload)+1))
	header[4] = byte(typ)

	t.wmu.Lock()
	defer t.wmu.Unlock()

	if _, err := t.conn.Write(header[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := t.conn.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

func (t *tcpTransport) Receive(ctx context.Context) (frameType, []byte, error) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = t.conn.SetReadDeadline(deadline)
	} else {
		_ = t.conn.SetReadDeadline(time.Time{})
	}

	var header [5]byte
	if _, err := io.ReadFull(t.r, header[:]); err != nil {
		return 0, nil, err
	}

	size := binary.BigEndian.Uint32(header[:4])
	if size == 0 {
		return 0, nil, errEmptyFrame
	}
	if size > MaxMessageSize+1 {
		return 0, nil, ErrMessageTooLarge
	}

	payload := make([]byte, size-1)
	if len(payload) > 0 {
		if _, err := io.ReadFull(t.r, payload); err != nil {
			return 0, nil, err
		}
	}
	return frameType(header[4]), payload, nil
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

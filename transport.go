package sqt

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

// frameType marks what a frame carries. Data frames become messages; ping and
// pong frames are consumed by the library and never reach Messages.
type frameType byte

const (
	frameData frameType = 1
	framePing frameType = 2
	framePong frameType = 3
)

var errEmptyFrame = errors.New("sqt: empty frame")

// transport is one established connection to a peer. QUIC opens a stream per
// frame, TCP writes length-prefixed frames on a single connection.
type transport interface {
	Send(ctx context.Context, typ frameType, payload []byte) error
	Receive(ctx context.Context) (frameType, []byte, error)
	Close() error
	RemoteAddr() net.Addr
}

// transportListener accepts established transports.
type transportListener interface {
	Accept(ctx context.Context) (transport, error)
	Close() error
	Addr() net.Addr
}

// quicTransport carries every frame of a connection over ONE bidirectional stream:
//
//	[4 bytes big-endian length][1 byte type][payload]
//
// A stream per frame was the original shape, and it does not survive a burst: a list of a hundred
// previews opened two hundred streams at once, hit the peer's MAX_STREAMS, blocked in OpenStreamSync and
// timed out - which looked like "pictures hang while everything else works" (31.10.2026). The frame type
// byte already tells what a frame is, so the stream count buys nothing.
//
// The stream is opened lazily, on the first frame: the server must accept connections even from a peer
// that connects and says nothing.
type quicTransport struct {
	conn   *quic.Conn
	client bool

	once  sync.Once
	st    *quic.Stream
	stErr error
	r     *bufio.Reader
	wmu   sync.Mutex
}

func newQUICTransport(conn *quic.Conn, client bool) *quicTransport {
	return &quicTransport{conn: conn, client: client}
}

// stream establishes the single stream of this connection once.
func (t *quicTransport) stream(ctx context.Context) (*quic.Stream, error) {
	t.once.Do(func() {
		if t.client {
			t.st, t.stErr = t.conn.OpenStreamSync(ctx)
		} else {
			t.st, t.stErr = t.conn.AcceptStream(ctx)
		}
		if t.stErr == nil {
			t.r = bufio.NewReaderSize(t.st, 64<<10)
		}
	})
	return t.st, t.stErr
}

func (t *quicTransport) Send(ctx context.Context, typ frameType, msg []byte) error {
	if len(msg) > MaxMessageSize {
		return ErrMessageTooLarge
	}
	st, err := t.stream(ctx)
	if err != nil {
		return err
	}

	if deadline, ok := ctx.Deadline(); ok {
		_ = st.SetWriteDeadline(deadline)
	} else {
		_ = st.SetWriteDeadline(time.Time{})
	}

	frame := make([]byte, 4+1+len(msg))
	binary.BigEndian.PutUint32(frame[:4], uint32(1+len(msg)))
	frame[4] = byte(typ)
	copy(frame[5:], msg)

	// Several goroutines may send at once; a frame must not be interleaved with another.
	t.wmu.Lock()
	defer t.wmu.Unlock()
	_, err = st.Write(frame)
	return err
}

func (t *quicTransport) Receive(ctx context.Context) (frameType, []byte, error) {
	st, err := t.stream(ctx)
	if err != nil {
		return 0, nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = st.SetReadDeadline(deadline)
	} else {
		_ = st.SetReadDeadline(time.Time{})
	}

	var length [4]byte
	if _, err := io.ReadFull(t.r, length[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(length[:])
	if n == 0 {
		return 0, nil, errEmptyFrame
	}
	if int(n) > MaxMessageSize+1 {
		return 0, nil, ErrMessageTooLarge
	}

	frame := make([]byte, n)
	if _, err := io.ReadFull(t.r, frame); err != nil {
		return 0, nil, err
	}
	return frameType(frame[0]), frame[1:], nil
}

func (t *quicTransport) Close() error {
	if t.st != nil {
		_ = t.st.Close()
	}
	return t.conn.CloseWithError(0, "closed")
}

func (t *quicTransport) RemoteAddr() net.Addr { return t.conn.RemoteAddr() }

type quicListener struct {
	ln *quic.Listener
}

func (l *quicListener) Accept(ctx context.Context) (transport, error) {
	conn, err := l.ln.Accept(ctx)
	if err != nil {
		return nil, err
	}
	return newQUICTransport(conn, false), nil
}

func (l *quicListener) Close() error   { return l.ln.Close() }
func (l *quicListener) Addr() net.Addr { return l.ln.Addr() }

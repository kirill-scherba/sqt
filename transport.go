package sqt

import (
	"context"
	"errors"
	"io"
	"net"

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

type quicTransport struct {
	conn *quic.Conn
}

func (t *quicTransport) Send(ctx context.Context, typ frameType, msg []byte) error {
	st, err := t.conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}

	if _, err := st.Write([]byte{byte(typ)}); err != nil {
		st.CancelWrite(0)
		return err
	}
	if len(msg) > 0 {
		if _, err := st.Write(msg); err != nil {
			st.CancelWrite(0)
			return err
		}
	}
	return st.Close()
}

func (t *quicTransport) Receive(ctx context.Context) (frameType, []byte, error) {
	st, err := t.conn.AcceptStream(ctx)
	if err != nil {
		return 0, nil, err
	}
	defer st.Close()

	// One byte of type plus at most MaxMessageSize of payload.
	data, err := io.ReadAll(io.LimitReader(st, MaxMessageSize+2))
	if err != nil {
		return 0, nil, err
	}
	if len(data) == 0 {
		return 0, nil, errEmptyFrame
	}

	payload := data[1:]
	if len(payload) > MaxMessageSize {
		return 0, nil, ErrMessageTooLarge
	}
	return frameType(data[0]), payload, nil
}

func (t *quicTransport) Close() error {
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
	return &quicTransport{conn: conn}, nil
}

func (l *quicListener) Close() error   { return l.ln.Close() }
func (l *quicListener) Addr() net.Addr { return l.ln.Addr() }

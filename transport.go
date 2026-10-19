package sqt

import (
	"context"
	"io"
	"net"

	"github.com/quic-go/quic-go"
)

// transport is one established connection to a peer. QUIC and TCP implement
// it differently: QUIC opens a stream per message, TCP writes length-prefixed
// frames on a single connection.
type transport interface {
	Send(ctx context.Context, msg []byte) error
	Receive(ctx context.Context) ([]byte, error)
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

func (t *quicTransport) Send(ctx context.Context, msg []byte) error {
	st, err := t.conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	if _, err := st.Write(msg); err != nil {
		st.CancelWrite(0)
		return err
	}
	return st.Close()
}

func (t *quicTransport) Receive(ctx context.Context) ([]byte, error) {
	st, err := t.conn.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	defer st.Close()
	return readMessage(st)
}

func (t *quicTransport) Close() error {
	return t.conn.CloseWithError(0, "closed")
}

func (t *quicTransport) RemoteAddr() net.Addr { return t.conn.RemoteAddr() }

func readMessage(st *quic.Stream) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(st, MaxMessageSize+1))
	if err != nil {
		return nil, err
	}
	if len(data) > MaxMessageSize {
		return nil, ErrMessageTooLarge
	}
	return data, nil
}

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

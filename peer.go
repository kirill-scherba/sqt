package sqt

import (
	"context"
	"io"
	"net"
	"sync"

	"github.com/quic-go/quic-go"
)

// Peer is one end of an established QUIC connection. The same type is used
// by the server for every accepted connection and by the client returned
// from Dial, so both sides can send and receive messages symmetrically.
type Peer struct {
	id   string
	conn *quic.Conn

	messages chan []byte
	done     chan struct{}

	closeOnce sync.Once

	mu            sync.Mutex
	closeErr      error
	closedLocally bool
}

// newPeer wraps an established QUIC connection and starts the receive loop.
func newPeer(id string, conn *quic.Conn) *Peer {
	p := &Peer{
		id:       id,
		conn:     conn,
		messages: make(chan []byte, defaultMessagesBuffer),
		done:     make(chan struct{}),
	}
	go p.readLoop()
	return p
}

// ID returns the server-assigned identifier. It is empty on the dialing side:
// the client does not need to know how the server named it.
func (p *Peer) ID() string { return p.id }

// RemoteAddr returns the address of the remote endpoint.
func (p *Peer) RemoteAddr() net.Addr { return p.conn.RemoteAddr() }

// Messages returns the channel of messages received from the remote side.
// The channel is closed when the connection is closed.
func (p *Peer) Messages() <-chan []byte { return p.messages }

// Done is closed when the connection is closed.
func (p *Peer) Done() <-chan struct{} { return p.done }

// Err returns the reason the connection ended: an idle timeout, a transport
// error, an application close from the remote side, or the error returned by
// AcceptStream. It returns nil while the connection is alive and after a local
// Close, because a local shutdown is not an error.
func (p *Peer) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closedLocally {
		return nil
	}
	return p.closeErr
}

// Send opens a new bidirectional stream, writes msg to it and closes the
// write side. Send is safe for concurrent use.
func (p *Peer) Send(ctx context.Context, msg []byte) error {
	if len(msg) > MaxMessageSize {
		return ErrMessageTooLarge
	}

	select {
	case <-p.done:
		return ErrPeerClosed
	default:
	}

	st, err := p.conn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	if _, err := st.Write(msg); err != nil {
		st.CancelWrite(0)
		return err
	}
	return st.Close()
}

// Close terminates the connection. It is safe to call more than once.
func (p *Peer) Close() error {
	p.mu.Lock()
	p.closedLocally = true
	p.mu.Unlock()
	return p.shutdown()
}

// shutdown closes the underlying QUIC connection. It is also called from the
// receive loop, so it must not mark the close as local.
func (p *Peer) shutdown() error {
	var err error
	p.closeOnce.Do(func() {
		close(p.done)
		err = p.conn.CloseWithError(0, "closed")
	})
	return err
}

// readLoop accepts incoming streams one by one and delivers their payloads
// to the messages channel. Reading sequentially keeps message order and gives
// natural backpressure through QUIC flow control.
func (p *Peer) readLoop() {
	defer close(p.messages)

	for {
		st, err := p.conn.AcceptStream(context.Background())
		if err != nil {
			p.mu.Lock()
			if !p.closedLocally {
				p.closeErr = err
			}
			p.mu.Unlock()
			_ = p.shutdown()
			return
		}

		msg, err := readMessage(st)
		_ = st.Close()
		if err != nil {
			continue
		}

		select {
		case p.messages <- msg:
		case <-p.done:
			return
		}
	}
}

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

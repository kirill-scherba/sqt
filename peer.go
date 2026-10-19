package sqt

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Heartbeat timings. They are variables so tests can shorten them.
var (
	heartbeatInterval = 30 * time.Second
	heartbeatTimeout  = 10 * time.Second
)

// Peer is one end of an established connection. The same type is used by the
// server for every accepted connection and by the client returned from Dial,
// so both sides can send and receive messages symmetrically.
type Peer struct {
	id string
	tr transport

	messages chan []byte
	done     chan struct{}

	closeOnce sync.Once

	mu            sync.Mutex
	closeErr      error
	closedLocally bool

	lastSent atomic.Int64
	pong     chan struct{}
}

// newPeer wraps an established transport and starts the receive and heartbeat
// loops.
func newPeer(id string, tr transport) *Peer {
	p := &Peer{
		id:       id,
		tr:       tr,
		messages: make(chan []byte, defaultMessagesBuffer),
		done:     make(chan struct{}),
		pong:     make(chan struct{}, 1),
	}
	p.lastSent.Store(time.Now().UnixNano())

	go p.readLoop()
	go p.heartbeat()

	return p
}

// ID returns the server-assigned identifier. It is empty on the dialing side:
// the client does not need to know how the server named it.
func (p *Peer) ID() string { return p.id }

// RemoteAddr returns the address of the remote endpoint.
func (p *Peer) RemoteAddr() net.Addr { return p.tr.RemoteAddr() }

// Messages returns the channel of messages received from the remote side.
// The channel is closed when the connection is closed.
func (p *Peer) Messages() <-chan []byte { return p.messages }

// Done is closed when the connection is closed.
func (p *Peer) Done() <-chan struct{} { return p.done }

// Err returns the reason the connection ended: an idle timeout, a heartbeat
// timeout, a transport error, an application close from the remote side, or a
// read error. It returns nil while the connection is alive and after a local
// Close, because a local shutdown is not an error.
func (p *Peer) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closedLocally {
		return nil
	}
	return p.closeErr
}

// Send writes a message to the remote side. Send is safe for concurrent use.
func (p *Peer) Send(ctx context.Context, msg []byte) error {
	if len(msg) > MaxMessageSize {
		return ErrMessageTooLarge
	}

	select {
	case <-p.done:
		return ErrPeerClosed
	default:
	}

	if err := p.tr.Send(ctx, frameData, msg); err != nil {
		return err
	}
	p.lastSent.Store(time.Now().UnixNano())
	return nil
}

// Close terminates the connection. It is safe to call more than once.
func (p *Peer) Close() error {
	p.mu.Lock()
	p.closedLocally = true
	p.mu.Unlock()
	return p.shutdown()
}

// shutdown closes the underlying transport. It is also called from the
// receive and heartbeat loops, so it must not mark the close as local.
func (p *Peer) shutdown() error {
	var err error
	p.closeOnce.Do(func() {
		close(p.done)
		err = p.tr.Close()
	})
	return err
}

// readLoop receives frames one by one. Data frames become messages; ping and
// pong frames are answered here and never reach the application.
func (p *Peer) readLoop() {
	defer close(p.messages)

	for {
		typ, msg, err := p.tr.Receive(context.Background())
		if err != nil {
			p.setCloseErr(err)
			_ = p.shutdown()
			return
		}

		switch typ {
		case frameData:
			p.signalPong()
			select {
			case p.messages <- msg:
			case <-p.done:
				return
			}

		case framePing:
			p.signalPong()
			ctx, cancel := context.WithTimeout(context.Background(), heartbeatTimeout)
			_ = p.tr.Send(ctx, framePong, nil)
			cancel()

		case framePong:
			p.signalPong()
		}
	}
}

// heartbeat keeps the connection active with a payload-bearing frame. Kernel
// TCP keepalive and QUIC PING are packets, but some middleboxes only count
// application data as activity.
func (p *Peer) heartbeat() {
	tick := heartbeatInterval / 2
	if tick > time.Second {
		tick = time.Second
	}
	if tick < 10*time.Millisecond {
		tick = 10 * time.Millisecond
	}

	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	var pingAt time.Time

	for {
		select {
		case <-p.done:
			return

		case now := <-ticker.C:
			if !pingAt.IsZero() {
				select {
				case <-p.pong:
					pingAt = time.Time{}
				default:
					if now.Sub(pingAt) > heartbeatTimeout {
						p.setCloseErr(ErrHeartbeatTimeout)
						_ = p.shutdown()
						return
					}
				}
				continue
			}

			if now.Sub(time.Unix(0, p.lastSent.Load())) < heartbeatInterval {
				continue
			}

			// Drop a stale pong before asking for a fresh one.
			select {
			case <-p.pong:
			default:
			}

			ctx, cancel := context.WithTimeout(context.Background(), heartbeatTimeout)
			err := p.tr.Send(ctx, framePing, nil)
			cancel()
			if err != nil {
				p.setCloseErr(err)
				_ = p.shutdown()
				return
			}
			pingAt = now
		}
	}
}

func (p *Peer) signalPong() {
	select {
	case p.pong <- struct{}{}:
	default:
	}
}

func (p *Peer) setCloseErr(err error) {
	p.mu.Lock()
	if !p.closedLocally && p.closeErr == nil {
		p.closeErr = err
	}
	p.mu.Unlock()
}

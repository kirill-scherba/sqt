// Package delivery implements at-least-once command delivery on top of sqt.
//
// The transport only moves bytes and reconnects. This package adds what a
// command needs to survive a broken path: a stable client name, a server-side
// queue, acknowledgements and client-side deduplication.
package delivery

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/kirill-scherba/sqt"
)

const (
	sendTimeout = 5 * time.Second
	retryAfter  = 3 * time.Second
)

// entry is a queued command and the time of the last delivery attempt.
type entry struct {
	text   string
	sentAt time.Time
}

// Wire messages.
type helloMsg struct {
	Hello string `json:"hello"`
}

type cmdMsg struct {
	Cmd  uint64 `json:"cmd"`
	Text string `json:"text"`
}

type ackMsg struct {
	Ack uint64 `json:"ack"`
}

// Server keeps a per-name queue of commands and delivers them to whichever
// peer currently identifies itself as that name. Commands stay in the queue
// until the client acknowledges them, so a reconnect or a lost ack causes a
// resend, not a lost command.
type Server struct {
	mu      sync.Mutex
	next    uint64
	pending map[string]map[uint64]entry
	peers   map[string]*sqt.Peer

	// OnData is called for messages from a client that are not acks.
	OnData func(name, text string)
}

// NewServer returns an empty delivery server.
func NewServer() *Server {
	return &Server{
		next:    1,
		pending: make(map[string]map[uint64]entry),
		peers:   make(map[string]*sqt.Peer),
	}
}

// Enqueue stores a command for name and sends it immediately when the client
// is connected. It returns the command identifier.
func (s *Server) Enqueue(name, text string) uint64 {
	s.mu.Lock()
	id := s.next
	s.next++

	if s.pending[name] == nil {
		s.pending[name] = make(map[uint64]entry)
	}
	s.pending[name][id] = entry{text: text}
	peer := s.peers[name]
	s.mu.Unlock()

	if peer != nil {
		if err := s.send(name, peer, id, text); err != nil {
			// The peer is dead but the transport has not noticed yet. Closing
			// it forces an immediate reconnect instead of waiting for the
			// heartbeat.
			_ = peer.Close()
		}
	}

	return id
}

// Names returns the names of the currently connected clients.
func (s *Server) Names() []string {
	s.mu.Lock()
	names := make([]string, 0, len(s.peers))
	for name := range s.peers {
		names = append(names, name)
	}
	s.mu.Unlock()

	sort.Strings(names)
	return names
}

// Pending reports how many commands are waiting for a client. Useful in tests
// and diagnostics.
func (s *Server) Pending(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending[name])
}

// Handle serves one peer until it disconnects: it waits for the hello frame,
// registers the name, resends everything unacknowledged and processes acks.
func (s *Server) Handle(ctx context.Context, peer *sqt.Peer) {
	name, ok := s.hello(ctx, peer)
	if !ok {
		return
	}

	s.mu.Lock()
	s.peers[name] = peer
	pending := make(map[uint64]string, len(s.pending[name]))
	for id, e := range s.pending[name] {
		pending[id] = e.text
	}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		if s.peers[name] == peer {
			delete(s.peers, name)
		}
		s.mu.Unlock()
	}()

	for id, text := range pending {
		if err := s.send(name, peer, id, text); err != nil {
			_ = peer.Close()
			return
		}
	}

	for {
		select {
		case <-ctx.Done():
			return

		case msg, ok := <-peer.Messages():
			if !ok {
				return
			}

			var ack ackMsg
			if err := json.Unmarshal(msg, &ack); err == nil && ack.Ack != 0 {
				s.mu.Lock()
				delete(s.pending[name], ack.Ack)
				s.mu.Unlock()
				continue
			}

			if s.OnData != nil {
				s.OnData(name, string(msg))
			}
		}
	}
}

func (s *Server) hello(ctx context.Context, peer *sqt.Peer) (string, bool) {
	select {
	case <-ctx.Done():
		return "", false

	case msg, ok := <-peer.Messages():
		if !ok {
			return "", false
		}

		var hello helloMsg
		if err := json.Unmarshal(msg, &hello); err != nil || hello.Hello == "" {
			return "", false
		}
		return hello.Hello, true
	}
}

// Run starts the retry loop. Commands that were not acknowledged are sent
// again every retryAfter, independently of the connection state: a silent
// dead path does not stop delivery, it only delays it.
func (s *Server) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.retry()
		}
	}
}

func (s *Server) retry() {
	type job struct {
		name string
		peer *sqt.Peer
		id   uint64
		text string
	}

	now := time.Now()

	s.mu.Lock()
	var jobs []job
	for name, entries := range s.pending {
		peer := s.peers[name]
		if peer == nil {
			continue
		}
		for id, e := range entries {
			if now.Sub(e.sentAt) >= retryAfter {
				jobs = append(jobs, job{name: name, peer: peer, id: id, text: e.text})
			}
		}
	}
	s.mu.Unlock()

	for _, j := range jobs {
		if err := s.send(j.name, j.peer, j.id, j.text); err != nil {
			_ = j.peer.Close()
		}
	}
}

func (s *Server) send(name string, peer *sqt.Peer, id uint64, text string) error {
	msg, err := json.Marshal(cmdMsg{Cmd: id, Text: text})
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
	err = peer.Send(ctx, msg)
	cancel()

	if name != "" {
		s.mu.Lock()
		if e, ok := s.pending[name][id]; ok {
			e.sentAt = time.Now()
			s.pending[name][id] = e
		}
		s.mu.Unlock()
	}

	return err
}

// Client identifies itself by name on every connection, executes each command
// once and acknowledges it. The seen set survives reconnects, so a resent
// command is not executed twice.
type Client struct {
	name    string
	handler func(id uint64, text string)

	mu   sync.Mutex
	seen map[uint64]struct{}
}

// NewClient returns a client with the given stable name and command handler.
func NewClient(name string, handler func(id uint64, text string)) *Client {
	return &Client{
		name:    name,
		handler: handler,
		seen:    make(map[uint64]struct{}),
	}
}

// Handle runs one connection: sends hello, then executes and acknowledges
// commands until the peer closes.
func (c *Client) Handle(ctx context.Context, peer *sqt.Peer) {
	hello, err := json.Marshal(helloMsg{Hello: c.name})
	if err != nil {
		return
	}

	sendCtx, cancel := context.WithTimeout(ctx, sendTimeout)
	_ = peer.Send(sendCtx, hello)
	cancel()

	for {
		select {
		case <-ctx.Done():
			return

		case msg, ok := <-peer.Messages():
			if !ok {
				return
			}

			var cmd cmdMsg
			if err := json.Unmarshal(msg, &cmd); err != nil || cmd.Cmd == 0 {
				continue
			}

			if c.first(cmd.Cmd) && c.handler != nil {
				c.handler(cmd.Cmd, cmd.Text)
			}

			ack, err := json.Marshal(ackMsg{Ack: cmd.Cmd})
			if err != nil {
				continue
			}
			ackCtx, ackCancel := context.WithTimeout(ctx, sendTimeout)
			_ = peer.Send(ackCtx, ack)
			ackCancel()
		}
	}
}

// first reports whether the command was not seen before and remembers it.
func (c *Client) first(id uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.seen[id]; ok {
		return false
	}
	c.seen[id] = struct{}{}
	return true
}

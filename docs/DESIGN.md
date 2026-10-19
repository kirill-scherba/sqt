# DESIGN — sqt

## Goal

A minimal library that lets a process behind NAT receive data over an
already-established outbound connection to a host with a public IP. It is not a
port forwarder and not a VPN: it moves opaque messages, and the application
decides what they mean.

## Components

| Path | Purpose |
| --- | --- |
| `sqt.go` | Package constants, errors, defaults |
| `peer.go` | Symmetric endpoint: send/receive over QUIC streams |
| `server.go` | QUIC listener, peer registry, sequential IDs |
| `client.go` | Dial helper |
| `internal/devtls` | Throwaway self-signed certificate for tests |
| `cmd/server` | Test application: listener + console |
| `cmd/client` | Test application: dial, receive, reply |

## Wire protocol

- Transport: QUIC v1 over UDP, TLS 1.3.
- ALPN: `sqt/1`.
- One message = one bidirectional stream.
  - Sender: `OpenStreamSync`, `Write(payload)`, `Close` (FIN).
  - Receiver: `AcceptStream`, read until EOF.
- No headers, no framing, no message IDs. A message is exactly the stream
  payload.
- Empty messages are valid.

## Transports

Two transports implement one internal interface (`Send`, `Receive`, `Close`,
`RemoteAddr`), so `Peer`, `Server` and `Dial` are transport-agnostic.

**QUIC.** One bidirectional stream per message, FIN terminates it. Multiplexed,
no head-of-line blocking, built-in keepalive and idle timeout. ALPN `sqt/1`.

**TCP/TLS.** One TLS connection, frames of `[4-byte length][1-byte type]
[payload]`; the length covers the type and the payload. Writes are serialized
with a mutex because `Send` is safe for concurrent use. ALPN `sqt/1` is
negotiated but framing is what matters. The transport is chosen with
`Network: "quic"` or `"tcp"`; the zero value is QUIC.

Both transports carry a type byte: `data` becomes a message, `ping` and `pong`
are consumed by `Peer`. QUIC writes it as the first byte of the stream, TCP as
part of the frame header.

TCP is the fallback for networks that recognise QUIC and throttle it: the byte
stream looks like HTTPS to a middlebox. The cost is head-of-line blocking and
the loss of per-message multiplexing, which does not matter for message sizes
this library targets.

## Liveness

`Peer` runs a heartbeat independent of the transport: after 30 seconds without
outgoing traffic it sends a `ping` and expects a `pong` within 10 seconds.
This exists because packet-level liveness is not enough. QUIC PING frames and
TCP keepalive probes are empty packets; some networks only reset their idle
timers on payload-bearing traffic and silently drop the flow otherwise.
Observed on a Hetzner path: a TCP connection with keepalive armed on both ends
died after about ten minutes of silence.

A missing pong closes the connection with `ErrHeartbeatTimeout`, which gives
`Client.Run` a reason to reconnect instead of waiting for a kernel timeout.

## Reconnect

`Client.Run` owns the reconnect loop, not the transports: dial, hand the peer
to the handler, wait for the handler to return, close, back off, repeat.
Backoff starts at one second, doubles to thirty, carries up to 20% jitter and
resets after a minute of stable work. Messages in flight during a disconnect
are lost, and commands may be delivered twice after a reconnect; the library
does not pretend otherwise. Stable identity is an application concern: the
client sends its name in the first message, the server maps it to the current
peer.

## Command delivery

The transport is deliberately dumb: it moves bytes and reconnects. Command
delivery is a separate layer (`internal/delivery`) because it needs semantics
the transport cannot provide — a stable client identity, a queue that outlives
a connection, acknowledgements and deduplication.

The failure model is "any connection can die at any moment". Under that model
the only useful guarantee is at-least-once with idempotent execution: a command
may arrive twice, but the client executes it once. The reference layer keeps
the seen ids in memory, which covers reconnects; persistence across process
restarts is left to the application.

## Connection lifecycle

1. Client dials `host:port`; TLS verifies the server certificate (or is skipped
   with `-insecure` in the test client).
2. Server accepts the connection, assigns a decimal ID starting at `1`, creates
   a `Peer` and publishes it on `Server.Peers()`.
3. Both sides run an accept loop. Any incoming stream is read fully and
   delivered on `Peer.Messages()`.
4. `Peer.Done()` is closed when the connection ends; `Messages()` is closed
   right after.
5. The server removes the peer from its registry on disconnect.

## Design decisions

**Streams instead of framing.** QUIC already provides message boundaries per
stream, flow control and multiplexing. A length-prefixed protocol on a single
stream would duplicate that and add head-of-line blocking.

**Sequential accept loop per peer.** Messages from one peer are read one at a
time, which preserves order and provides backpressure. Throughput is not the
goal; a future version can spawn a goroutine per stream if needed.

**Opaque payloads.** Addressing (`1, hello`), replies and correlation are test
application concerns. The library must not impose a schema.

**Symmetric `Peer`.** The server treats each accepted connection and the
dialing side identically, so one implementation serves both directions.

**Keepalive 15s, idle timeout 60s.** Keeps NAT mappings alive while detecting
dead peers reasonably fast.

**Server-assigned IDs.** The server numbers peers in connection order. The
dialing side does not need to know its ID; if the application needs stable
names, it puts them in the first message.

## Non-goals (v1)

- Authentication and authorization.
- TLS certificate distribution / pinning helpers.
- Reconnection and session resumption.
- Delivery guarantees beyond a single QUIC connection.
- Broadcast and message routing.
- Configuration files.

## Relay

`internal/udprelay` and `cmd/relay` forward datagrams between clients and an
upstream server. This is deliberately not a QUIC-aware proxy: QUIC is encrypted
end to end, so an intermediate host can only forward packets or terminate TLS.
Forwarding is enough, because QUIC demultiplexes connections by connection ID
and treats the relay as an ordinary NAT hop.

One upstream socket per client is required: with a single shared socket all
clients would present the same source address to the server and path handling
would see one constantly moving endpoint. Sessions expire after an idle
timeout; QUIC keepalive traffic refreshes them.

The kernel alternative (nftables DNAT plus masquerade) is faster and needs no
process, but requires root, `ip_forward`, conntrack and a working nftables, and
cannot translate between address families. The userspace relay works anywhere
Go runs, crosses IPv4 and IPv6 in either direction, needs no privileges for
ports above 1024, and logs sessions.

## Possible next steps

- Optional `Peer.Name` from a first handshake message.
- Optional reconnect loop in a higher-level client wrapper.
- Per-stream goroutines and a configurable concurrency limit.
- Metrics: connected peers, messages, bytes.

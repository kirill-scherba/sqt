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

## Possible next steps

- Optional `Peer.Name` from a first handshake message.
- Optional reconnect loop in a higher-level client wrapper.
- Per-stream goroutines and a configurable concurrency limit.
- Metrics: connected peers, messages, bytes.

# CONTEXT — sqt

## What this is

`sqt` (simple QUIC tunnel) is a Go library for message transport between a
server with a public IP and clients behind NAT. The client dials the server;
after that both sides can send messages over QUIC streams. The package is
transport only: payloads are opaque bytes.

The repository also contains two test applications built on the library:
`cmd/server` and `cmd/client`.

## Current state

- Module: `github.com/kirill-scherba/sqt`
- Dependencies: `github.com/quic-go/quic-go v0.64.1`
- Go: 1.27
- Library API: `NewServer`, `Server.Run`, `Server.Peers`, `Server.Peer`,
  `Server.PeerIDs`, `Server.Close`, `Dial`, `Peer.Send`, `Peer.Messages`,
  `Peer.Done`, `Peer.Close`.
- ALPN `sqt/1`, max message 1 MiB, keepalive 15s, idle timeout 60s.
- Tests: exchange, ordering, size limit, shutdown propagation.
- Both directions are exercised: the test client has a stdin console and can
  send at any time, not only as a reply.

## Decisions

- One message = one QUIC stream; no custom framing.
- Symmetric `Peer` type for both sides.
- Server assigns sequential IDs; not part of the payload protocol.
- No auth, no reconnection, no configs in v1.
- TLS: real certificate on the server (Let's Encrypt), self-signed generator
  for local tests.

## Local layout

`~/go/src/github.com/kirill-scherba/sqt`

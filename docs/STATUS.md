# STATUS — sqt

## Done

- Library skeleton: `Server`, `Peer`, `Dial`.
- Message transport over QUIC streams, both directions.
- Sequential server-side peer IDs and registry.
- Self-signed certificate helper for local tests.
- Test applications `cmd/server` (listener + console) and `cmd/client`
  (console + receive + reply).
- Messages flow in both directions: the client sends spontaneous input lines
  and automatic replies; the server receives both.
- SIGTERM stops the server even while the console is blocked on stdin.
- Headless mode: the server no longer depends on stdin. The console runs only
  with a real terminal (`-console auto`), or is forced with `-console on|off`.
  Previously a server started with stdin at EOF closed immediately.
- `Peer.Err()` reports why a connection ended; the client prints the lifetime
  and the reason (`idle timeout`, `remote application close`, transport error).
- Typed frames (data/ping/pong) in both transports. Wire format changed:
  server and client must be rebuilt together.
- Heartbeat: 30s interval, 10s pong timeout, works for QUIC and TCP. Closes the
  connection with `ErrHeartbeatTimeout` instead of waiting for a kernel timeout.
- `sqt.Client` with `Run` and automatic reconnect: 1s to 30s exponential
  backoff with jitter, reset after a minute of stable work. Covering tests.
- `-keepalive` and `-idle` flags on both applications, and matching
  `KeepAlivePeriod` / `IdleTimeout` fields in the library configs.
- Observed in the wild: a remote Windows client loses the path after the first
  exchange and reports `timeout: no recent network activity`. The QUIC layer
  stops receiving packets, so the cause is the UDP path (filtering, NAT or
  provider policy), not the application.
- Confirmed environment: on a Hetzner path from Russia the DPI passes a few
  packets and then cuts the flow. The same client works through a tunnel to a
  neighbouring Hetzner host over IPv6. Keepalive, IPv4/IPv6 and bidirectional
  streams were verified independently, so the transport itself is healthy.
- Unit/integration tests: `go test ./...` green.
- End-to-end manual run verified in both directions: client `from client` →
  server, server `1, hi there` → client, client reply → server, disconnect
  event.

- TCP/TLS transport (`-network tcp`): one connection, length-prefixed frames,
  serialized writes. The same `Peer` API; tests run against both transports.
- Timers tuned for interactive use: heartbeat 5s/3s, reconnect backoff
  500ms..3s, delivery retry every 3s.
- `internal/delivery`: reference at-least-once delivery (hello/queue/ack/dedup)
  used by both test applications. Chaos test drops the connection every 250ms
  while commands are enqueued; each command executes exactly once on QUIC and
  TCP.
- `cmd/relay` (userspace UDP relay) with per-client upstream sockets, idle
  session expiry, `-v` logging and tests: forwarding in both directions,
  client separation, context cancellation.
- End-to-end verified through the relay: client → relay → server, messages in
  both directions, a 20s idle gap, clean shutdown.

## Known limitations

- Max message size is 1 MiB.
- No reconnect: if the connection drops, the client process must be restarted.
- No authentication; anyone who can reach the UDP port and pass TLS can connect.
- Sequential message reads per peer; no per-stream parallelism.

## Next

- Decide whether the server-side test application should expose a separate
  operator CLI (currently the console is embedded in `cmd/server`).
- Consider a handshake message carrying a client name.
- Add a reconnect wrapper if real applications need it.

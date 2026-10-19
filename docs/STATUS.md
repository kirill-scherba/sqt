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
- Unit/integration tests: `go test ./...` green.
- End-to-end manual run verified in both directions: client `from client` →
  server, server `1, hi there` → client, client reply → server, disconnect
  event.

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

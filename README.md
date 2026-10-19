# sqt — simple QUIC tunnel

A small Go library for message transport between a server with a public IP and
clients behind NAT. The client dials the server over QUIC; after that either
side can open a stream and send a message. The library carries opaque bytes and
knows nothing about their meaning.

Two test binaries are included: `cmd/server` and `cmd/client`. They only
demonstrate the API.

## How it works

```
                 QUIC :4433, ALPN sqt/1
  client  ────────────────────────────────►  server
  (NAT)   ◄────────────────────────────────  (public IP)
              stream = one message
```

The client keeps the connection alive with QUIC keepalive. Because QUIC allows
both endpoints to open streams, the server can push a message to a client that
is behind NAT without any port forwarding: the packets travel over the existing
connection.

Each message is carried in its own bidirectional stream. The sender writes the
payload and closes the write side (FIN); the receiver reads until EOF. That
gives natural framing, backpressure and multiplexing without a custom
length-prefix protocol.

## Library

```go
import "github.com/kirill-scherba/sqt"

// server side
srv, err := sqt.NewServer(sqt.ServerConfig{
    Addr:      ":4433",
    TLSConfig: tlsConfig, // must contain a certificate
})
go srv.Run(ctx)

for peer := range srv.Peers() {
    go func(p *sqt.Peer) {
        for msg := range p.Messages() {
            _ = p.Send(ctx, append([]byte("reply: "), msg...))
        }
    }(peer)
}

// client side
peer, err := sqt.Dial(ctx, sqt.ClientConfig{
    Addr:      "sqt.example.com:4433",
    TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13},
})
for msg := range peer.Messages() {
    log.Printf("received: %s", msg)
    _ = peer.Send(ctx, []byte("ok"))
}
```

`Peer` is symmetric: the same type is returned by `Dial` and handed out by
`Server.Peers()`.

## Test applications

Build:

```sh
go build -o bin/sqt-server ./cmd/server
go build -o bin/sqt-client ./cmd/client
```

On Windows, run the same commands with Go installed (binaries get the `.exe`
suffix), or cross-compile from Linux:

```sh
GOOS=windows GOARCH=amd64 go build -o bin/sqt-server.exe ./cmd/server
GOOS=windows GOARCH=amd64 go build -o bin/sqt-client.exe ./cmd/client
```

Run the server with a real certificate:

```sh
./bin/sqt-server -addr :4433 -cert /etc/letsencrypt/live/example.com/fullchain.pem \
    -key /etc/letsencrypt/live/example.com/privkey.pem
```

For local testing, omit `-cert` and `-key`: the server generates a self-signed
certificate and prints its SHA-256 fingerprint.

The interactive console is enabled only when stdin is a terminal. Use
`-console on` to force it (for example when driving the server from a pipe) or
`-console off` for a headless process. A server attached to a terminal you are
going to close dies with that terminal; use systemd or `nohup` together with
`-console off` instead.

Run the client:

```sh
./bin/sqt-client -addr sqt.example.com:4433
./bin/sqt-client -addr 127.0.0.1:4433 -insecure   # self-signed server
```

Both applications accept `-keepalive` and `-idle` to tune the QUIC timers:

```sh
./bin/sqt-client -addr sqt.example.com:4433 -keepalive 5s -idle 30s
```

Server console:

```
> ls
clients: 1
> 1, hello
sent to 1: hello
received from 1: reply: hello
> quit
```

Client output:

```
connected to 127.0.0.1:4433
received: hello
reply sent: reply: hello
```

The client also reads stdin: every non-empty line is sent to the server
immediately, so it can push messages without waiting for anything. `quit`
closes the connection.

```
> from client
sent: from client
```

## Relay

`cmd/relay` forwards UDP datagrams between clients and a server the clients
cannot reach directly. It is not a QUIC proxy: packets pass through untouched
and QUIC does not notice the extra hop.

```sh
go build -o bin/sqt-relay ./cmd/relay

# on the intermediate host
./bin/sqt-relay -listen :4433 -upstream 65.х.х.х:4433 -v
```

Point the client at the relay:

```sh
./bin/sqt-client -addr relay.example.com:4433
```

Every client gets its own upstream socket, so the server sees separate
endpoints instead of one shared address. Sessions are closed after `-idle`
(default 2m) without traffic; the QUIC keepalive keeps them alive.

TLS uses the name the client dials. If the server certificate must verify,
point that name (A/AAAA record) at the relay: the certificate still belongs to
the server and the check succeeds. Dialing the relay by IP literal requires
`-insecure`, because the certificate has no IP SAN.

## When a client disconnects

The client prints how long the connection lived and why it ended:

```
connected to sqt.example.com:4433 (keepalive 15s, idle timeout 1m0s)
disconnected after 1m0s: timeout: no recent network activity
```

| Reason | Meaning |
| --- | --- |
| `Application error 0x0 (remote): closed` | The other side closed the connection: server shutdown, `quit`, or a crash that sent CONNECTION_CLOSE. |
| `timeout: no recent network activity` | The idle timeout fired: nothing was received for 60s even though keepalive PINGs are sent every 15s, so the path is gone. |
| Transport / stateless reset errors | The network reset the UDP flow, or the peer disappeared without a close frame. |
| No reason, `disconnected after ...` | The local process closed the peer itself (signal, `quit`). |

Keepalive is what keeps NAT mappings alive. If the server process is stopped —
even by a stop signal such as SIGTTIN when a background process reads from a
terminal — packets stop being answered and the client sees an idle timeout
about a minute later.

If messages stop flowing in one direction while the connection is still fresh,
the UDP path is being filtered somewhere between the hosts. Cheap checks:
another port (`:443` is treated best by middleboxes), a shorter keepalive
(`-keepalive 5s`), and the server log for the matching disconnect reason. When
filtering is persistent, QUIC cannot get through at all and the transport has
to be TCP/TLS.

Some networks pass the first few packets and then drop the flow. This was
observed on Hetzner paths from Russia: the handshake and the first exchange
succeed, then the client receives nothing and reports an idle timeout while
keepalive keeps sending PINGs. No timer tuning helps in that case; the options
are a tunnel, another provider, or a TCP/TLS transport.

## Limits

- Messages are opaque byte slices, at most 1 MiB (`sqt.MaxMessageSize`).
- No authentication, addressing, request/response correlation or reconnection —
  those belong to the layer above.
- The client does not learn its server-assigned ID; IDs are only visible on the
  server side.

# ris_live_bmp

A Go application that bridges [RIPE RIS Live](https://ris-live.ripe.net/) and a [BMP (BGP Monitoring Protocol)](https://www.rfc-editor.org/rfc/rfc7854) server. It consumes the real-time BGP message stream from RIS Live, wraps each message in a BMP v3 Route Monitoring envelope, and forwards it over TCP to a BMP-capable collector such as [gobmp](https://github.com/sbezverk/gobmp).

## How It Works

1. **Connect to RIS Live** -- opens an HTTP streaming connection to `https://ris-live.ripe.net/v1/stream/?format=json`, which delivers BGP updates as newline-delimited JSON.
2. **Connect to BMP server** -- dials a TCP connection to the configured BMP server (default `localhost:5000`).
3. **Stream processing loop** -- each incoming JSON line is decoded into a `RIS` struct containing the peer address, peer ASN, timestamp, and the hex-encoded raw BGP message.
4. **BMP message assembly** (per message, in a goroutine):
   - Builds a **BMP Common Header** (version 3, Route Monitor message type).
   - Builds a **BMP Per-Peer Header** populated with the peer address (IPv4 or IPv6), peer ASN, BGP ID, and timestamp.
   - Decodes the hex-encoded raw BGP payload.
   - Serializes the headers and appends the raw payload to form the complete BMP message (Common Header 6 bytes + Per-Peer Header 42 bytes + BGP payload).
5. **Forward** -- writes the assembled BMP message to the BMP server.

## Prerequisites

- Go 1.15 or later

## Installation

```bash
git clone https://github.com/SergeKrier/ris_live_bmp.git
cd ris_live_bmp
go build -o ris_live_bmp .
```

## Usage

```bash
./ris_live_bmp [flags]
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-bmp-server` | `localhost:5000` | Address and port of the target BMP server |
| `-bgp-id` | `1.1.1.1` | BGP Identifier used in outgoing BMP messages |

### Example

```bash
# Forward RIS Live messages to a gobmp instance running on 192.168.1.10:5000
./ris_live_bmp -bmp-server 192.168.1.10:5000 -bgp-id 10.0.0.1
```

## Dependencies

| Module | Purpose |
|--------|---------|
| [github.com/golang/glog](https://github.com/golang/glog) | Leveled logging |
| [github.com/sbezverk/gobmp](https://github.com/sbezverk/gobmp) | BMP message structures and serialization |

## License

See [LICENSE](LICENSE) for details.
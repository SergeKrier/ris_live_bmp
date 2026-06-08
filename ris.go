// Package main implements a bridge between the RIPE NCC RIS Live streaming API
// and a BMP (BGP Monitoring Protocol) server. It consumes real-time BGP routing
// messages from the RIS Live firehose, wraps each one in a BMP Route Monitoring
// message (version 3), and forwards them over TCP to a configurable BMP server
// such as gobmp.
package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/bmp"
)

// stream is the RIPE RIS Live SSE endpoint that delivers BGP update messages
// as newline-delimited JSON.
var stream = "https://ris-live.ripe.net/v1/stream/?format=json"

// Command-line configuration for the target BMP server and the BGP Identifier
// used in outgoing BMP messages.
var (
	bmpAddress string
	bgpID      string
)

func init() {
	flag.StringVar(&bmpAddress, "bmp-server", "localhost:5000", "IP or DNS address and port of gobmp server")
	flag.StringVar(&bgpID, "bgp-id", "1.1.1.1", "BGP ID to use in BMP messages")
}

// RISData defines structure of data portion of RIS message
type RISData struct {
	Type      string      `json:"type,omitempty"`
	Timestamp json.Number `json:"timestamp,omitempty"`
	Peer      string      `json:"peer,omitempty"`
	PeerASN   string      `json:"peer_asn,omitempty"`
	ID        string      `json:"id,omitempty"`
	Raw       string      `json:"raw,omitempty"`
	Host      string      `json:"host,omitempty"`
}

// RIS defines RIS message format
type RIS struct {
	Type string   `json:"type,omitempty"`
	Data *RISData `json:"data,omitempty"`
}

// Message represents a BMP message composed of a Common Header and a
// Per-Peer Header. The raw BGP payload is appended during serialization.
type Message struct {
	CommonHeader  *bmp.CommonHeader
	PerPeerHeader *bmp.PerPeerHeader
}

// main connects to the RIS Live stream and the BMP server, then enters a loop
// that reads each JSON message from the stream, converts it into a BMP Route
// Monitoring message in a separate goroutine, and writes it to the BMP server.
// Any goroutine error causes the program to exit.
func main() {
	flag.Parse()
	_ = flag.Set("logtostderr", "true")
	// Open an HTTP connection to the RIS Live SSE stream.
	ris, err := http.Get(stream)
	if err != nil {
		glog.Errorf("failed to connect to RIS source with error: %+v", err)
		os.Exit(1)
	}
	defer ris.Body.Close()
	// Establish a TCP connection to the target BMP server.
	bmpSrv, err := net.Dial("tcp", bmpAddress)
	if err != nil {
		glog.Errorf("failed to connect to destination with error: %+v", err)
		os.Exit(1)
	}
	defer bmpSrv.Close()
	glog.Infof("connection to bmp %v established", bmpSrv.RemoteAddr())

	// Read the HTTP response body line-by-line; each line is one JSON message.
	reader := bufio.NewReader(ris.Body)
	errorCh := make(chan error)
	for {
		m := &RIS{}
		b, err := reader.ReadBytes('\n')
		if err != nil {
			glog.Errorf("failed to read message with error: %+v", err)
			os.Exit(1)
		}
		// Process each message in its own goroutine to avoid blocking the reader.
		go func(b []byte, errorCh chan error) {
			// Decode the JSON-encoded RIS Live message.
			if err := json.Unmarshal(b, m); err != nil {
				glog.Errorf("failed to decode streamed message with error: %+v", err)
				errorCh <- err
				return
			}
			// Build the BMP Route Monitoring message from the RIS data.
			bmpMsg := Message{}
			bmpMsg.CommonHeader = &bmp.CommonHeader{
				Version:     3,
				MessageType: bmp.RouteMonitorMsg,
			}
			bmpMsg.PerPeerHeader = &bmp.PerPeerHeader{
				PeerType:          0, // *  Peer Type = 0: Global Instance Peer
				PeerDistinguisher: make([]byte, 8),
				PeerAddress:       make([]byte, 16),
				PeerBGPID:         net.ParseIP(bgpID).To4(),
				PeerTimestamp:     make([]byte, 8),
			}
			// Populating peer address
			pa := net.ParseIP(m.Data.Peer)
			if pa.To4() != nil {
				bmpMsg.PerPeerHeader.FlagV = false
				copy(bmpMsg.PerPeerHeader.PeerAddress[12:16], pa.To4())
			} else if pa.To16() != nil {
				bmpMsg.PerPeerHeader.FlagV = true
				copy(bmpMsg.PerPeerHeader.PeerAddress, pa.To16())
			} else {
				glog.Warningf("invalid peer address %s", m.Data.Peer)
				errorCh <- err
				return
			}
			// Populating Peer ASN
			asn, err := strconv.Atoi(m.Data.PeerASN)
			if err != nil {
				glog.Warningf("invalid peer asn %s", m.Data.PeerASN)
			}
			bmpMsg.PerPeerHeader.PeerAS = int32(asn)

			// Populating peer timestamp
			t := strings.Split(m.Data.Timestamp.String(), ".")
			sec, _ := strconv.Atoi(t[0])
			msec := 0
			if len(t) > 1 {
				msec, _ = strconv.Atoi(t[1])
			}
			binary.BigEndian.PutUint32(bmpMsg.PerPeerHeader.PeerTimestamp[0:4], uint32(sec))
			binary.BigEndian.PutUint32(bmpMsg.PerPeerHeader.PeerTimestamp[4:8], uint32(msec))

			// Decode the hex-encoded raw BGP message payload.
			raw, err := hex.DecodeString(m.Data.Raw)
			if err != nil {
				glog.Warningf("invalid raw data, failed to decode with error: %+v", err)
			}

			// Assemble the final BMP message: Common Header (6 bytes) +
			// Per-Peer Header (42 bytes) + raw BGP payload.
			bmpMsg.CommonHeader.MessageLength = int32(6 + 42 + len(raw))
			b1, _ := bmpMsg.CommonHeader.Serialize()
			b2, _ := bmpMsg.PerPeerHeader.Serialize()
			fullMsg := make([]byte, bmpMsg.CommonHeader.MessageLength)
			copy(fullMsg, b1)
			copy(fullMsg[6:], b2)
			copy(fullMsg[48:], raw)

			// Send the assembled BMP message to the server.
			if _, err := bmpSrv.Write(fullMsg); err != nil {
				glog.Errorf("fail to write to server %+v with error: %+v", bmpSrv.RemoteAddr(), err)
				errorCh <- err
				return
			}
		}(b, errorCh)
		// Check if any goroutine reported error, if it is the case, then exit the loop
		select {
		case err := <-errorCh:
			glog.Errorf("go routine failed with error: %+v, exiting the loop", err)
			os.Exit(1)
		default:
		}
	}
}

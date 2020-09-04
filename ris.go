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

var stream = "https://ris-live.ripe.net/v1/stream/?format=json"

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

type Message struct {
	CommonHeader  *bmp.CommonHeader
	PerPeerHeader *bmp.PerPeerHeader
}

func main() {
	flag.Parse()
	_ = flag.Set("logtostderr", "true")
	ris, err := http.Get(stream)
	if err != nil {
		glog.Errorf("failed to connect to RIS source with error: %+v", err)
		os.Exit(1)
	}
	defer ris.Body.Close()
	bmpSrv, err := net.Dial("tcp", bmpAddress)
	if err != nil {
		glog.Errorf("failed to connect to destination with error: %+v", err)
		os.Exit(1)
	}
	defer bmpSrv.Close()
	glog.Infof("connection to bmp %v established", bmpSrv.RemoteAddr())

	reader := bufio.NewReader(ris.Body)
	errorCh := make(chan error)
	for {
		m := &RIS{}
		b, err := reader.ReadBytes('\n')
		if err != nil {
			glog.Errorf("failed to read message with error: %+v", err)
			os.Exit(1)
		}
		go func(b []byte, errorCh chan error) {
			if err := json.Unmarshal(b, m); err != nil {
				glog.Errorf("failed to decode streamed message with error: %+v", err)
				errorCh <- err
				return
			}
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

			raw, err := hex.DecodeString(m.Data.Raw)
			if err != nil {
				glog.Warningf("invalid raw data, failed to decode with error: %+v", err)
			}

			bmpMsg.CommonHeader.MessageLength = int32(6 + 42 + len(raw))
			b1, _ := bmpMsg.CommonHeader.Serialize()
			b2, _ := bmpMsg.PerPeerHeader.Serialize()
			fullMsg := make([]byte, bmpMsg.CommonHeader.MessageLength)
			copy(fullMsg, b1)
			copy(fullMsg[6:], b2)
			copy(fullMsg[48:], raw)

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

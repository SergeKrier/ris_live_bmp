/*
 Serge Krier - August 2020
*/

package main

import (
        "fmt"
        "net/http"
        "bufio"
        "os"
        "encoding/json"
)
/*
"type":"ris_message",
"data":
 {"timestamp":1598790597.83,
  "peer":"2001:504:1::a539:7143:1",
  "peer_asn":"397143",
  "id":"11-2001-504-1-a539-7143-1-141235381",
  "host":"rrc11",
  "type":"UPDATE",
  "path":[397143,6939,18881,18881],
  "origin":"igp",
  "announcements":
      [{"next_hop":"2001:504:1::a539:7143:1",
         "prefixes":["2804:7f5:8000::/33","2804:7f4:8000::/34","2804:7f4::/33","2804:7f2:8000::/33","2804:1b2:8000::/35","2804:1b0::/35"]},{"ne\
xt_hop":"fe80::216:3eff:fe56:dd03","prefixes":["2804:7f5:8000::/33","2804:7f4:8000::/34","2804:7f4::/33","2804:7f2:8000::/33","2804:1b2:8000::/\
35","2804:1b0::/35"]}],

  "raw":"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF00A4020000008D900E00490002012020010504000100000000A53971430001FE8000000000000002163EFFFE56DD0300212804\
07F58022280407F48021280407F40021280407F28023280401B28023280401B00040010100400212020400060F5700001B1B000049C1000049C1E0202400060F570000006500000\
00200060F57000000660000034800060F570000006700000001"}

*/

func main() {

        url := "http://ris-live.ripe.net/v1/stream/?format=json"

        var msg map[string]map[string]interface{}


        resp, err := http.Get(url)
        if err != nil {
                fmt.Fprintf(os.Stderr, "fetch: %v\n", err)
                os.Exit(1)
        }
        reader := bufio.NewReader(resp.Body)
        for {
                line, _ := reader.ReadBytes('\n')
                json.Unmarshal(line, &msg)
                data := msg["data"]
                raw := data["raw"]
                host := data["host"]
                peer := data["peer"]
                peer_asn := data["peer_asn"]

                fmt.Println(host, peer, "ASN", peer_asn, data["type"], raw)

        }
}

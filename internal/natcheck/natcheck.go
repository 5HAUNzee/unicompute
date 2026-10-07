// Package natcheck classifies the NAT in front of this machine by sending STUN
// binding requests to two different servers from ONE UDP socket and comparing
// the public mappings that come back.
//
//   same public port for both servers -> endpoint-independent mapping ("cone")
//                                        -> direct UDP hole punching very likely
//   different ports                    -> symmetric NAT
//                                        -> hole punching usually fails, TURN needed
//   no response at all                 -> outbound UDP is blocked on this network
package natcheck

import (
	"fmt"
	"net"
	"time"

	"github.com/pion/stun/v3"
)

type Result struct {
	LocalIP     string   `json:"local_ip"`
	LocalPort   int      `json:"local_port"`
	Mapped      []string `json:"mapped"`
	PublicIP    string   `json:"public_ip"`
	Type        string   `json:"type"` // open | cone | symmetric | blocked | unknown
	Punchable   bool     `json:"punchable"`
	Description string   `json:"description"`
}

// Servers are probed in order from the same local socket.
var Servers = []string{"stun.l.google.com:19302", "stun1.l.google.com:19302"}

// Run performs the classification. It never returns an error; failures are
// reported in Result.Type so callers can always display something.
func Run(timeout time.Duration) *Result {
	r := &Result{Type: "unknown", LocalIP: outboundIP()}

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	if err != nil {
		r.Type = "blocked"
		r.Description = "cannot open a UDP socket: " + err.Error()
		return r
	}
	defer conn.Close()
	r.LocalPort = conn.LocalAddr().(*net.UDPAddr).Port

	for _, s := range Servers {
		m, err := bind(conn, s, timeout)
		if err != nil {
			continue
		}
		r.Mapped = append(r.Mapped, m)
	}

	if len(r.Mapped) == 0 {
		r.Type = "blocked"
		r.Description = "No STUN response from any server. Outbound UDP appears to be blocked on this network; WebRTC cannot work here."
		return r
	}

	host, port, _ := net.SplitHostPort(r.Mapped[0])
	r.PublicIP = host

	if host == r.LocalIP {
		r.Type = "open"
		r.Punchable = true
		r.Description = "This machine has a public IP directly (no NAT). Direct P2P is guaranteed."
		return r
	}

	if len(r.Mapped) >= 2 {
		_, port2, _ := net.SplitHostPort(r.Mapped[1])
		if port != port2 {
			r.Type = "symmetric"
			r.Punchable = false
			r.Description = "Symmetric NAT: a different public port is allocated per destination. Direct hole punching usually FAILS; expect TURN relay fallback (or test from a different network)."
			return r
		}
	}

	r.Type = "cone"
	r.Punchable = true
	r.Description = "Endpoint-independent mapping (cone NAT). Direct UDP hole punching is very likely to succeed."
	return r
}

func bind(conn *net.UDPConn, server string, timeout time.Duration) (string, error) {
	raddr, err := net.ResolveUDPAddr("udp4", server)
	if err != nil {
		return "", err
	}
	req := stun.MustBuild(stun.TransactionID, stun.BindingRequest)
	if _, err := conn.WriteToUDP(req.Raw, raddr); err != nil {
		return "", err
	}
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, 1500)
	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			return "", err
		}
		res := &stun.Message{Raw: append([]byte(nil), buf[:n]...)}
		if err := res.Decode(); err != nil {
			continue
		}
		if res.TransactionID != req.TransactionID {
			continue // a late reply to an earlier probe
		}
		var xa stun.XORMappedAddress
		if err := xa.GetFrom(res); err == nil {
			return fmt.Sprintf("%s:%d", xa.IP, xa.Port), nil
		}
		var ma stun.MappedAddress
		if err := ma.GetFrom(res); err == nil {
			return fmt.Sprintf("%s:%d", ma.IP, ma.Port), nil
		}
		return "", fmt.Errorf("no mapped address in STUN response")
	}
}

// outboundIP returns the local interface address the OS would use to reach the
// internet. No packet is sent; a UDP "dial" only selects a route.
func outboundIP() string {
	c, err := net.Dial("udp4", "8.8.8.8:80")
	if err != nil {
		return ""
	}
	defer c.Close()
	return c.LocalAddr().(*net.UDPAddr).IP.String()
}

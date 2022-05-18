// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package loadbalancer

import (
	"log"
	"net"
	"time"

	"inet.af/tcpproxy"

	"github.com/talos-systems/go-loadbalancer/upstream"
)

type lbTarget struct {
	list            *upstream.List
	logger          *log.Logger
	dialTimeout     time.Duration
	keepAlivePeriod time.Duration
	tcpUserTimeout  time.Duration
}

func (target *lbTarget) HandleConn(conn net.Conn) {
	upstreamBackend, err := target.list.Pick()
	if err != nil {
		target.logger.Printf("no upstreams available, closing connection from %s", conn.RemoteAddr())
		conn.Close() //nolint: errcheck

		return
	}

	upstream := upstreamBackend.(node) //nolint:errcheck,forcetypeassert

	target.logger.Printf("proxying connection %s -> %s", conn.RemoteAddr(), upstream.address)

	upstreamTarget := tcpproxy.To(upstream.address)
	upstreamTarget.DialTimeout = target.dialTimeout
	upstreamTarget.KeepAlivePeriod = target.keepAlivePeriod
	upstreamTarget.TCPUserTimeout = target.tcpUserTimeout
	upstreamTarget.OnDialError = func(src net.Conn, dstDialErr error) {
		src.Close() //nolint: errcheck

		target.logger.Printf("error dialing upstream %s: %s", upstream.address, dstDialErr)

		target.list.Down(upstreamBackend)
	}

	upstreamTarget.HandleConn(conn)

	target.logger.Printf("closing connection %s -> %s", conn.RemoteAddr(), upstream.address)
}

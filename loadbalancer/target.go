// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package loadbalancer

import (
	"net"
	"time"

	"github.com/siderolabs/tcpproxy"
	"go.uber.org/zap"

	"github.com/siderolabs/go-loadbalancer/upstream"
)

type lbTarget struct {
	list            *upstream.List[node]
	logger          *zap.Logger
	dialTimeout     time.Duration
	keepAlivePeriod time.Duration
	tcpUserTimeout  time.Duration
}

func (target *lbTarget) HandleConn(conn net.Conn) {
	upstreamBackend, err := target.list.Pick()
	if err != nil {
		target.logger.Info(
			"no upstreams available, closing connection",
			zap.String("remote_addr", conn.RemoteAddr().String()),
		)
		conn.Close() //nolint: errcheck

		return
	}

	target.logger.Debug(
		"proxying connection",
		zap.String("remote_addr", conn.RemoteAddr().String()),
		zap.String("upstream_addr", upstreamBackend.address),
	)

	upstreamTarget := tcpproxy.To(upstreamBackend.address)
	upstreamTarget.DialTimeout = target.dialTimeout
	upstreamTarget.KeepAlivePeriod = target.keepAlivePeriod
	upstreamTarget.TCPUserTimeout = target.tcpUserTimeout
	upstreamTarget.OnDialError = func(src net.Conn, dstDialErr error) {
		src.Close() //nolint: errcheck

		target.logger.Info(
			"error dialing upstream",
			zap.String("upstream_addr", upstreamBackend.address),
			zap.Error(dstDialErr),
		)

		target.list.Down(upstreamBackend)
	}

	upstreamTarget.HandleConn(conn)

	target.logger.Debug(
		"closing connection",
		zap.String("remote_addr", conn.RemoteAddr().String()),
		zap.String("upstream_addr", upstreamBackend.address),
	)
}

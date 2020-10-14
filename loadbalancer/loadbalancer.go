// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package loadbalancer provides simple TCP loadbalancer.
package loadbalancer

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"inet.af/tcpproxy"

	"github.com/talos-systems/go-loadbalancer/upstream"
)

// TCP is a simple loadbalancer for TCP connections across a set of upstreams.
//
// Healthcheck is defined as TCP dial attempt by default.
//
// Zero value of TCP is a valid proxy, use `AddRoute` to install load balancer for
// address.
//
// Usage: call Run() to start lb and wait for shutdown, call Close() to shutdown lb.
type TCP struct {
	tcpproxy.Proxy

	DialTimeout     time.Duration
	KeepAlivePeriod time.Duration
	TCPUserTimeout  time.Duration

	Logger *log.Logger

	routes map[string]*upstream.List
}

type lbUpstream struct {
	upstream string
	logger   *log.Logger
}

func (upstream lbUpstream) HealthCheck(ctx context.Context) error {
	d := net.Dialer{}

	c, err := d.DialContext(ctx, "tcp", upstream.upstream)
	if err != nil {
		upstream.logger.Printf("healthcheck failed for %q: %s", upstream.upstream, err)

		return err
	}

	return c.Close()
}

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

	upstream := upstreamBackend.(lbUpstream) //nolint: errcheck

	target.logger.Printf("proxying connection %s -> %s", conn.RemoteAddr(), upstream.upstream)

	upstreamTarget := tcpproxy.To(upstream.upstream)
	upstreamTarget.DialTimeout = target.dialTimeout
	upstreamTarget.KeepAlivePeriod = target.keepAlivePeriod
	upstreamTarget.TCPUserTimeout = target.tcpUserTimeout
	upstreamTarget.OnDialError = func(src net.Conn, dstDialErr error) {
		src.Close() //nolint: errcheck

		target.logger.Printf("error dialing upstream %s: %s", upstream.upstream, dstDialErr)

		target.list.Down(upstreamBackend)
	}

	upstreamTarget.HandleConn(conn)

	target.logger.Printf("closing connection %s -> %s", conn.RemoteAddr(), upstream.upstream)
}

// AddRoute installs load balancer route from listen address ipAddr to list of upstreams.
//
// TCP automatically does background health checks for the upstreams and picks only healthy
// ones. Healthcheck is simple Dial attempt.
func (t *TCP) AddRoute(ipPort string, upstreamAddrs []string, options ...upstream.ListOption) error {
	if t.Logger == nil {
		t.Logger = log.New(log.Writer(), "", log.Flags())
	}

	if t.routes == nil {
		t.routes = make(map[string]*upstream.List)
	}

	upstreams := make([]upstream.Backend, len(upstreamAddrs))
	for i := range upstreams {
		upstreams[i] = lbUpstream{
			upstream: upstreamAddrs[i],
			logger:   t.Logger,
		}
	}

	list, err := upstream.NewList(upstreams, options...)
	if err != nil {
		return err
	}

	t.routes[ipPort] = list

	t.Proxy.AddRoute(ipPort, &lbTarget{
		list:            list,
		logger:          t.Logger,
		dialTimeout:     t.DialTimeout,
		keepAlivePeriod: t.KeepAlivePeriod,
		tcpUserTimeout:  t.TCPUserTimeout,
	})

	return nil
}

// ReconcileRoute updates the list of upstreamAddrs for the specified route (ipPort).
func (t *TCP) ReconcileRoute(ipPort string, upstreamAddrs []string) error {
	if t.routes == nil {
		t.routes = make(map[string]*upstream.List)
	}

	list := t.routes[ipPort]
	if list == nil {
		return fmt.Errorf("handler not registered for %q", ipPort)
	}

	upstreams := make([]upstream.Backend, len(upstreamAddrs))
	for i := range upstreams {
		upstreams[i] = lbUpstream{
			upstream: upstreamAddrs[i],
			logger:   t.Logger,
		}
	}

	list.Reconcile(upstreams)

	return nil
}

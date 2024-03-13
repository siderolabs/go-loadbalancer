// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package loadbalancer provides simple TCP loadbalancer.
package loadbalancer

import (
	"errors"
	"fmt"
	"time"

	"github.com/siderolabs/gen/xslices"
	"github.com/siderolabs/tcpproxy"
	"go.uber.org/zap"

	"github.com/siderolabs/go-loadbalancer/upstream"
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

	Logger *zap.Logger

	routes map[string]*upstream.List[node]

	DialTimeout     time.Duration
	KeepAlivePeriod time.Duration
	TCPUserTimeout  time.Duration
}

// IsRouteHealthy checks if the route has at least one upstream available.
func (t *TCP) IsRouteHealthy(ipPort string) (bool, error) {
	list, ok := t.routes[ipPort]
	if !ok {
		return false, fmt.Errorf("no routes with ipPort %s registered", ipPort)
	}

	_, err := list.Pick()
	if err != nil {
		if errors.Is(err, upstream.ErrNoUpstreams) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

// AddRoute installs load balancer route from listen address ipAddr to list of upstreams.
//
// TCP automatically does background health checks for the upstreams and picks only healthy
// ones. Healthcheck is simple Dial attempt.
//
// AddRoute should be called before Start().
func (t *TCP) AddRoute(ipPort string, upstreamAddrs []string, options ...upstream.ListOption) error {
	if t.Logger == nil {
		t.Logger = zap.Must(zap.NewProduction())
	}

	if t.routes == nil {
		t.routes = make(map[string]*upstream.List[node])
	}

	upstreams := xslices.Map(upstreamAddrs, func(addr string) node {
		return node{
			address: addr,
			logger:  t.Logger,
		}
	})

	// We can still override tiers if we want to on layers above.
	options = append([]upstream.ListOption{upstream.WithTiers(0, upstream.Tier(len(mins)-1), 1)}, options...)

	list, err := upstream.NewListWithCmp(upstreams, func(a, b node) bool {
		return a.address == b.address
	}, options...)
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
//
// ReconcileRoute can be called when the loadbalancer is running.
func (t *TCP) ReconcileRoute(ipPort string, upstreamAddrs []string) error {
	if t.routes == nil {
		return fmt.Errorf("no routes installed")
	}

	list := t.routes[ipPort]
	if list == nil {
		return fmt.Errorf("handler not registered for %q", ipPort)
	}

	upstreams := xslices.Map(upstreamAddrs, func(addr string) node {
		return node{
			address: addr,
			logger:  t.Logger,
		}
	})

	list.Reconcile(upstreams)

	return nil
}

// Close the load balancer and stop health checks on upstreams.
func (t *TCP) Close() error {
	if err := t.Proxy.Close(); err != nil {
		return err
	}

	if t.routes == nil {
		return nil
	}

	for _, upstream := range t.routes {
		upstream.Shutdown()
	}

	return nil
}

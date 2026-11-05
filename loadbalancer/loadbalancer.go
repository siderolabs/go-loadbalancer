// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package loadbalancer provides simple TCP loadbalancer.
package loadbalancer

import (
	"errors"
	"fmt"
	"iter"
	"slices"
	"time"

	"github.com/siderolabs/gen/xiter"
	"github.com/siderolabs/tcpproxy"
	"go.uber.org/zap"

	"github.com/siderolabs/go-loadbalancer/upstream"
)

// TCP is a simple loadbalancer for TCP connections across a set of upstreams.
//
// Healthcheck is defined as TCP dial attempt by default, it can be overridden with HealthCheck.
//
// Zero value of TCP is a valid proxy, use `AddRoute` to install load balancer for
// address.
//
// Usage: call Run() to start lb and wait for shutdown, call Close() to shutdown lb.
type TCP struct {
	tcpproxy.Proxy

	Logger *zap.Logger

	routes map[string]*route

	// HealthCheck overrides the default upstream health check (TCP dial attempt, see [DialHealthCheck]).
	//
	// HealthCheck is captured by AddRoute: changing it afterwards only affects routes added later,
	// upstreams of the existing routes (including ones added via ReconcileRoute) keep using the health check
	// which was set when the route was added.
	//
	// The same HealthCheck is shared by all routes, so it must be safe for concurrent use (see [HealthCheckFunc]).
	HealthCheck HealthCheckFunc

	// TierThresholds overrides the health check latency thresholds used to calculate upstream tiers.
	//
	// An upstream gets tier i if its health check took at least TierThresholds[i] (and less than TierThresholds[i+1]),
	// a health check faster than TierThresholds[0] gets tier 0. Lower tiers are preferred when picking an upstream.
	// Thresholds should be strictly increasing, with 1 to 11 entries (one entry disables latency-based tiering).
	//
	// Default thresholds are tuned for a TCP dial attempt: 0, 100µs, 1ms, 10ms, 100ms, 1s.
	//
	// TierThresholds is captured by AddRoute, same as HealthCheck.
	TierThresholds []time.Duration

	DialTimeout     time.Duration
	KeepAlivePeriod time.Duration
	TCPUserTimeout  time.Duration
}

// IsRouteHealthy checks if the route has at least one upstream available.
func (t *TCP) IsRouteHealthy(ipPort string) (bool, error) {
	r, ok := t.routes[ipPort]
	if !ok {
		return false, fmt.Errorf("no routes with ipPort %s registered", ipPort)
	}

	_, err := r.list.Pick()
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
// ones. Healthcheck is simple Dial attempt, unless overridden with HealthCheck.
//
// AddRoute should be called before Start().
func (t *TCP) AddRoute(ipPort string, upstreamAddrs iter.Seq[string], options ...upstream.ListOption) error {
	if t.Logger == nil {
		t.Logger = zap.Must(zap.NewProduction())
	}

	if t.routes == nil {
		t.routes = map[string]*route{}
	}

	if upstreamAddrs == nil {
		upstreamAddrs = xiter.Empty[string]
	}

	r := &route{
		logger:         t.Logger,
		healthCheck:    t.HealthCheck,
		tierThresholds: slices.Clone(t.TierThresholds),
	}

	if r.healthCheck == nil {
		r.healthCheck = DialHealthCheck
	}

	if r.tierThresholds == nil {
		r.tierThresholds = defaultTierThresholds
	}

	if err := validateTierThresholds(r.tierThresholds); err != nil {
		return err
	}

	// We can still override tiers if we want to on layers above.
	maxTier := upstream.Tier(len(r.tierThresholds) - 1)
	options = slices.Insert(options, 0, upstream.WithTiers(0, maxTier, min(1, maxTier)))

	list, err := upstream.NewListWithCmp(
		xiter.Map(r.newNode, upstreamAddrs),
		func(a, b node) bool { return a.address == b.address },
		options...)
	if err != nil {
		return err
	}

	r.list = list

	t.routes[ipPort] = r

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
func (t *TCP) ReconcileRoute(ipPort string, upstreamAddrs iter.Seq[string]) error {
	if t.routes == nil {
		return fmt.Errorf("no routes installed")
	}

	r := t.routes[ipPort]
	if r == nil {
		return fmt.Errorf("handler not registered for %q", ipPort)
	}

	if upstreamAddrs == nil {
		upstreamAddrs = xiter.Empty[string]
	}

	r.list.Reconcile(xiter.Map(r.newNode, upstreamAddrs))

	return nil
}

// route is a load balancer route: the list of upstreams with the settings captured when the route was added.
type route struct {
	list           *upstream.List[node]
	logger         *zap.Logger
	healthCheck    HealthCheckFunc
	tierThresholds []time.Duration
}

func (r *route) newNode(addr string) node {
	return node{
		address:        addr,
		logger:         r.logger,
		healthCheck:    r.healthCheck,
		tierThresholds: r.tierThresholds,
	}
}

// Close the load balancer and stop health checks on upstreams.
func (t *TCP) Close() error {
	if err := t.Proxy.Close(); err != nil {
		return err
	}

	for _, r := range t.routes {
		r.list.Shutdown()
	}

	return nil
}

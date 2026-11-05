// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package loadbalancer

import (
	"context"
	"fmt"
	"slices"
	"time"

	"go.uber.org/zap"

	"github.com/siderolabs/go-loadbalancer/upstream"
)

// HealthCheckFunc checks the health of the upstream at the given address (host:port).
//
// The context passed to the function carries the health check timeout (see [upstream.WithHealthcheckTimeout]),
// and it is canceled when the load balancer is closed, so the function should return promptly once the context is done.
// The function should release all resources it acquired (connections, goroutines) before it returns.
//
// A nil error marks the upstream as healthy, the time taken by the function is used to calculate the upstream tier.
// The default tier thresholds are tuned for a TCP dial attempt, and the default health check timeout is 100ms,
// so a heavier health check should come with a longer timeout and matching tier thresholds (see [TCP.TierThresholds]).
//
// Each route runs health checks of its upstreams sequentially in its own goroutine, and the same function
// is shared by all routes of the [TCP] load balancer, so the function must be safe for concurrent use.
type HealthCheckFunc func(ctx context.Context, address string) error

// DialHealthCheck is the default health check: a TCP connection attempt to the upstream.
func DialHealthCheck(ctx context.Context, address string) error {
	c, err := probeDialer().DialContext(ctx, "tcp", address)
	if err != nil {
		return err
	}

	return c.Close()
}

type node struct {
	logger         *zap.Logger
	healthCheck    HealthCheckFunc
	address        string // host:port
	tierThresholds []time.Duration
}

func (upstream node) HealthCheck(ctx context.Context) (upstream.Tier, error) {
	start := time.Now()
	err := upstream.healthCheck(ctx, upstream.address)
	elapsed := time.Since(start)

	if err != nil {
		upstream.logger.Warn("healthcheck failed", zap.String("address", upstream.address), zap.Error(err))
	}

	return calcTier(upstream.tierThresholds, err, elapsed)
}

// defaultTierThresholds are tuned for a TCP dial attempt.
var defaultTierThresholds = []time.Duration{
	0,
	time.Millisecond / 10,
	time.Millisecond,
	10 * time.Millisecond,
	100 * time.Millisecond,
	1 * time.Second,
}

// maxTierThresholds is limited by the max tier supported by [upstream.WithTiers].
const maxTierThresholds = 11

func validateTierThresholds(thresholds []time.Duration) error {
	if len(thresholds) == 0 || len(thresholds) > maxTierThresholds {
		return fmt.Errorf("number of tier thresholds should be between 1 and %d, got %d", maxTierThresholds, len(thresholds))
	}

	for i := 1; i < len(thresholds); i++ {
		if thresholds[i] <= thresholds[i-1] {
			return fmt.Errorf("tier thresholds should be strictly increasing, got %s after %s", thresholds[i], thresholds[i-1])
		}
	}

	return nil
}

func calcTier(thresholds []time.Duration, err error, elapsed time.Duration) (upstream.Tier, error) {
	if err != nil {
		// preserve old tier
		return -1, err
	}

	for i, threshold := range slices.Backward(thresholds) {
		if elapsed >= threshold {
			return upstream.Tier(i), nil
		}
	}

	// faster than the first threshold
	return 0, nil
}

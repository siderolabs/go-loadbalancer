// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package loadbalancer

import (
	"context"
	"log"
	"time"

	"github.com/siderolabs/go-loadbalancer/upstream"
)

type node struct {
	logger  *log.Logger
	address string // host:port
}

func (upstream node) HealthCheck(ctx context.Context) (upstream.Tier, error) {
	start := time.Now()
	err := upstream.healthCheck(ctx)
	elapsed := time.Since(start)

	return calcTier(err, elapsed)
}

func (upstream node) healthCheck(ctx context.Context) error {
	d := probeDialer()

	c, err := d.DialContext(ctx, "tcp", upstream.address)
	if err != nil {
		upstream.logger.Printf("healthcheck failed for %q: %s", upstream.address, err)

		return err
	}

	return c.Close()
}

var mins = []time.Duration{
	0, time.Millisecond,
	time.Millisecond,
	10 * time.Millisecond,
	100 * time.Millisecond,
	1 * time.Second,
}

func calcTier(err error, elapsed time.Duration) (upstream.Tier, error) {
	if err != nil {
		// preserve old tier
		return -1, err
	}

	for i := len(mins) - 1; i >= 0; i-- {
		if elapsed >= mins[i] {
			return upstream.Tier(i), nil
		}
	}

	// We should never get here, but there is no way to tell this to Go compiler.
	return upstream.Tier(len(mins)), err
}

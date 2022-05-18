// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package loadbalancer

import (
	"context"
	"log"
	"net"
)

type node struct {
	logger  *log.Logger
	address string // host:port
}

func (upstream node) HealthCheck(ctx context.Context) error {
	d := net.Dialer{}

	c, err := d.DialContext(ctx, "tcp", upstream.address)
	if err != nil {
		upstream.logger.Printf("healthcheck failed for %q: %s", upstream.address, err)

		return err
	}

	return c.Close()
}

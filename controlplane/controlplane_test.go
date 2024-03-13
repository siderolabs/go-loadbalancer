// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controlplane_test

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/siderolabs/gen/xslices"
	"github.com/siderolabs/go-retry/retry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap/zaptest"

	"github.com/siderolabs/go-loadbalancer/controlplane"
	"github.com/siderolabs/go-loadbalancer/upstream"
)

//nolint:govet
type mockUpstream struct {
	T        testing.TB
	Identity string

	addr string
	l    net.Listener
}

func (u *mockUpstream) Start() error {
	var err error

	u.l, err = net.Listen("tcp", "localhost:0")
	if err != nil {
		return err
	}

	u.addr = u.l.Addr().String()

	go u.serve()

	return nil
}

func (u *mockUpstream) serve() {
	for {
		c, err := u.l.Accept()
		if err != nil {
			return
		}

		_, err = c.Write([]byte(u.Identity))
		require.NoError(u.T, err)
		require.NoError(u.T, c.Close())
	}
}

func (u *mockUpstream) Close() {
	require.NoError(u.T, u.l.Close())
}

func TestLoadBalancer(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const (
		upstreamCount = 5
		pivot         = 2
	)

	upstreams := make([]mockUpstream, upstreamCount)
	for i := range upstreams {
		upstreams[i].T = t
		upstreams[i].Identity = strconv.Itoa(i)
		require.NoError(t, upstreams[i].Start())
	}

	upstreamAddrs := xslices.Map(upstreams, func(u mockUpstream) string { return u.addr })

	lb, err := controlplane.NewLoadBalancer(
		"localhost",
		0,
		zaptest.NewLogger(t),
		controlplane.WithHealthCheckOptions(
			// start with negative initlal score so that every healthcheck will be performed
			// at least once. It will also make upstream tiers.
			upstream.WithInitialScore(-1),
			upstream.WithHealthcheckInterval(10*time.Millisecond),
		),
	)
	require.NoError(t, err)

	upstreamCh := make(chan []string)

	require.NoError(t, lb.Start(upstreamCh))

	upstreamCh <- upstreamAddrs[:pivot]

	readIdentity := func() (int, error) {
		c, err := net.Dial("tcp", lb.Endpoint())
		if err != nil {
			return 0, retry.ExpectedError(err)
		}

		defer ensure(t, c.Close)

		id, err := io.ReadAll(c)
		if err != nil {
			return 0, retry.ExpectedError(err)
		} else if len(id) == 0 {
			return 0, retry.ExpectedErrorf("zero length response")
		}

		return strconv.Atoi(string(id))
	}

	assert.NoError(t, retry.Constant(10*time.Second, retry.WithUnits(30*time.Millisecond)).Retry(func() error {
		identity, err := readIdentity()
		if err != nil {
			return err
		}

		if identity < 0 || identity > pivot-1 {
			return fmt.Errorf("unexpected response: %d", identity)
		}

		return nil
	}))

	{
		healthy, err := lb.Healthy()
		require.NoError(t, err)

		assert.True(t, healthy)
	}

	// change the upstreams
	upstreamCh <- upstreamAddrs[pivot:]

	assert.NoError(t, retry.Constant(10*time.Second, retry.WithUnits(time.Second)).Retry(func() error {
		identity, err := readIdentity()
		if err != nil {
			return err
		}

		// upstreams are not changed immediately, there might be some stale responses
		if identity < pivot {
			return retry.ExpectedErrorf("unexpected response: %d", identity)
		}

		return nil
	}))

	{
		healthy, err := lb.Healthy()
		require.NoError(t, err)

		assert.True(t, healthy)
	}

	for i := range upstreams {
		upstreams[i].Close()
	}

	{
		err := retry.Constant(time.Second * 10).Retry(func() error {
			healthy, err := lb.Healthy()
			if err != nil {
				return err
			}

			if healthy {
				return retry.ExpectedErrorf("lb is still healthy")
			}

			return nil
		})

		require.NoError(t, err)
	}

	assert.NoError(t, lb.Shutdown())
}

func ensure(t *testing.T, closer func() error) {
	require.NoError(t, closer())
}

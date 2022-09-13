// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controlplane_test

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/talos-systems/go-retry/retry"
	"go.uber.org/goleak"

	"github.com/talos-systems/go-loadbalancer/controlplane"
)

type mockUpstream struct {
	addr string
	l    net.Listener

	identity string
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

		c.Write([]byte(u.identity)) //nolint: errcheck
		c.Close()                   //nolint: errcheck
	}
}

func (u *mockUpstream) Close() {
	u.l.Close() //nolint: errcheck
}

func TestLoadBalancer(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const (
		upstreamCount = 5
		pivot         = 2
	)

	upstreams := make([]mockUpstream, upstreamCount)
	for i := range upstreams {
		upstreams[i].identity = strconv.Itoa(i)
		require.NoError(t, upstreams[i].Start())
	}

	upstreamAddrs := make([]string, len(upstreams))
	for i := range upstreamAddrs {
		upstreamAddrs[i] = upstreams[i].addr
	}

	lb, err := controlplane.NewLoadBalancer("localhost", 0, os.Stderr)
	require.NoError(t, err)

	upstreamCh := make(chan []string)

	require.NoError(t, lb.Start(upstreamCh))

	upstreamCh <- upstreamAddrs[:pivot]

	readIdentity := func() (int, error) {
		c, err := net.Dial("tcp", lb.Endpoint())
		if err != nil {
			return 0, retry.ExpectedError(err)
		}

		defer c.Close() //nolint:errcheck

		id, err := io.ReadAll(c)
		if err != nil {
			return 0, retry.ExpectedError(err)
		}

		return strconv.Atoi(string(id))
	}

	assert.NoError(t, retry.Constant(10*time.Second, retry.WithUnits(time.Second)).Retry(func() error {
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

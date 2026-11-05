// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package loadbalancer_test

import (
	"context"
	"errors"
	"io"
	"net"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"go.uber.org/zap/zaptest"

	"github.com/siderolabs/go-loadbalancer/loadbalancer"
	"github.com/siderolabs/go-loadbalancer/upstream"
)

type mockUpstream struct {
	addr string
	l    net.Listener

	identity string
}

func (u *mockUpstream) Start() error {
	var err error

	u.l, err = (&net.ListenConfig{}).Listen(context.Background(), "tcp", "localhost:0")
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

func findListenAddress() (string, error) {
	u := mockUpstream{}

	if err := u.Start(); err != nil {
		return "", err
	}

	u.Close()

	return u.addr, nil
}

type TCPSuite struct {
	suite.Suite
}

func (suite *TCPSuite) TestReconcile() {
	const (
		upstreamCount = 5
		pivot         = 2
	)

	upstreams := make([]mockUpstream, upstreamCount)
	for i := range upstreams {
		upstreams[i].identity = strconv.Itoa(i)
		suite.Require().NoError(upstreams[i].Start())
	}

	upstreamAddrs := make([]string, len(upstreams))
	for i := range upstreamAddrs {
		upstreamAddrs[i] = upstreams[i].addr
	}

	listenAddr, err := findListenAddress()
	suite.Require().NoError(err)

	lb := &loadbalancer.TCP{Logger: zaptest.NewLogger(suite.T())}
	suite.Require().NoError(lb.AddRoute(
		listenAddr,
		slices.Values(upstreamAddrs[:pivot]),
		upstream.WithLowHighScores(-3, 3),
		upstream.WithInitialScore(1),
		upstream.WithScoreDeltas(-1, 1),
		upstream.WithHealthcheckInterval(time.Second),
		upstream.WithHealthcheckTimeout(100*time.Millisecond),
		upstream.WithTiers(0, 0, 0),
	))

	suite.Require().NoError(lb.Start())

	var wg sync.WaitGroup

	wg.Go(func() {
		lb.Wait() //nolint: errcheck
	})

	for i := range 5 * pivot {
		c, err := (&net.Dialer{}).DialContext(suite.T().Context(), "tcp", listenAddr)
		suite.Require().NoError(err)

		id, err := io.ReadAll(c)
		suite.Require().NoError(err)

		// load balancer should go round-robin across all the upstreams [0:pivot]
		suite.Assert().Equal([]byte(strconv.Itoa(i%pivot)), id)

		suite.Require().NoError(c.Close())
	}

	// reconcile the list
	suite.Require().NoError(lb.ReconcileRoute(listenAddr, slices.Values(upstreamAddrs[pivot:])))

	// bring down pre-pivot upstreams
	for i := range pivot {
		upstreams[i].Close()
	}

	upstreamsUsed := map[int64]int{}

	for i := range 10 * (upstreamCount - pivot) {
		c, err := (&net.Dialer{}).DialContext(suite.T().Context(), "tcp", listenAddr)
		suite.Require().NoError(err)

		id, err := io.ReadAll(c)
		suite.Require().NoError(err)

		// load balancer should go round-robin across all the upstreams [pivot:]
		no, err := strconv.ParseInt(string(id), 10, 32)
		suite.Require().NoError(err)

		suite.Assert().EqualValues(no, pivot+(i+pivot)%(upstreamCount-pivot))

		suite.Assert().Less(no, int64(upstreamCount))
		suite.Assert().GreaterOrEqual(no, int64(pivot))

		upstreamsUsed[no]++

		suite.Require().NoError(c.Close())
	}

	for _, count := range upstreamsUsed {
		suite.Assert().Equal(10, count)
	}

	suite.Require().NoError(lb.Close())
	wg.Wait()

	for i := range upstreams {
		upstreams[i].Close()
	}
}

func (suite *TCPSuite) TestBalancer() {
	const (
		upstreamCount   = 5
		failingUpstream = 1
	)

	upstreams := make([]mockUpstream, upstreamCount)
	for i := range upstreams {
		upstreams[i].identity = strconv.Itoa(i)
		suite.Require().NoError(upstreams[i].Start())
	}

	upstreamAddrs := make([]string, len(upstreams))
	for i := range upstreamAddrs {
		upstreamAddrs[i] = upstreams[i].addr
	}

	listenAddr, err := findListenAddress()
	suite.Require().NoError(err)

	lb := &loadbalancer.TCP{Logger: zaptest.NewLogger(suite.T())}
	suite.Require().NoError(lb.AddRoute(
		listenAddr,
		slices.Values(upstreamAddrs),
		upstream.WithLowHighScores(-3, 3),
		upstream.WithInitialScore(1),
		upstream.WithScoreDeltas(-1, 1),
		upstream.WithHealthcheckInterval(time.Second),
		upstream.WithHealthcheckTimeout(100*time.Millisecond),
		upstream.WithTiers(0, 0, 0),
	))

	suite.Require().NoError(lb.Start())

	var wg sync.WaitGroup

	wg.Go(func() {
		lb.Wait() //nolint: errcheck
	})

	for i := range 2 * upstreamCount {
		c, err := (&net.Dialer{}).DialContext(suite.T().Context(), "tcp", listenAddr)
		suite.Require().NoError(err)

		id, err := io.ReadAll(c)
		suite.Require().NoError(err)

		// load balancer should go round-robin across all the upstreams
		suite.Assert().Equal([]byte(strconv.Itoa(i%upstreamCount)), id)

		suite.Require().NoError(c.Close())
	}

	// bring down one upstream
	upstreams[failingUpstream].Close()

	j := 0
	failedRequests := 0

	for range 10 * upstreamCount {
		c, err := (&net.Dialer{}).DialContext(suite.T().Context(), "tcp", listenAddr)
		suite.Require().NoError(err)

		id, err := io.ReadAll(c)
		suite.Require().NoError(err)

		if len(id) == 0 {
			// hit failing upstream
			suite.Assert().Equal(failingUpstream, j%upstreamCount)

			failedRequests++

			continue
		}

		if j%upstreamCount == failingUpstream {
			j++
		}

		// load balancer should go round-robin across all the upstreams
		suite.Assert().Equal([]byte(strconv.Itoa(j%upstreamCount)), id)

		j++

		suite.Require().NoError(c.Close())
	}

	// worst case: score = 3 (highScore) to go to -1 requires 5 requests
	suite.Assert().Less(failedRequests, 5) // no more than 5 requests should fail

	suite.Require().NoError(lb.Close())
	wg.Wait()

	for i := range upstreams {
		upstreams[i].Close()
	}
}

func (suite *TCPSuite) TestScoreOnDial() {
	var upstream1 mockUpstream

	upstream1.identity = "one"
	suite.Require().NoError(upstream1.Start())

	listenAddr, err := findListenAddress()
	suite.Require().NoError(err)

	lb := &loadbalancer.TCP{Logger: zaptest.NewLogger(suite.T())}
	suite.Require().NoError(lb.AddRoute(
		listenAddr,
		slices.Values([]string{upstream1.addr}),
		upstream.WithLowHighScores(-3, 3),
		upstream.WithInitialScore(0),
		upstream.WithScoreDeltas(-1, 1),
		upstream.WithHealthcheckInterval(time.Hour), // only the initial health check runs
		upstream.WithHealthcheckTimeout(100*time.Millisecond),
		upstream.WithTiers(0, 0, 0),
	))

	suite.Require().NoError(lb.Start())

	var wg sync.WaitGroup

	wg.Go(func() {
		lb.Wait() //nolint: errcheck
	})

	// initial health check: score == 1, successful proxied connections don't affect the score
	for range 5 {
		c, err := (&net.Dialer{}).DialContext(suite.T().Context(), "tcp", listenAddr)
		suite.Require().NoError(err)

		id, err := io.ReadAll(c)
		suite.Require().NoError(err)
		suite.Assert().Equal([]byte("one"), id)

		suite.Require().NoError(c.Close())
	}

	// bring down the upstream: each failed dial lowers the score by one, so it takes
	// exactly 2 failed connections (1 -> -1) for the route to become unhealthy
	upstream1.Close()

	failedRequests := 0

	for {
		healthy, err := lb.IsRouteHealthy(listenAddr)
		suite.Require().NoError(err)

		if !healthy {
			break
		}

		c, err := (&net.Dialer{}).DialContext(suite.T().Context(), "tcp", listenAddr)
		suite.Require().NoError(err)

		id, err := io.ReadAll(c)
		suite.Require().NoError(err)
		suite.Assert().Empty(id)

		suite.Require().NoError(c.Close())

		failedRequests++
	}

	suite.Assert().Equal(2, failedRequests)

	suite.Require().NoError(lb.Close())
	wg.Wait()
}

func (suite *TCPSuite) TestCustomHealthCheck() {
	const upstreamCount = 3

	upstreams := make([]mockUpstream, upstreamCount)
	for i := range upstreams {
		upstreams[i].identity = strconv.Itoa(i)
		suite.Require().NoError(upstreams[i].Start())
	}

	t := suite.T()

	t.Cleanup(func() {
		for i := range upstreams {
			upstreams[i].Close()
		}
	})

	upstreamAddrs := make([]string, len(upstreams))
	for i := range upstreamAddrs {
		upstreamAddrs[i] = upstreams[i].addr
	}

	listenAddr, err := findListenAddress()
	suite.Require().NoError(err)

	// upstream 1 accepts TCP connections, but it fails the custom health check
	var (
		unhealthyAddr   atomic.Pointer[string]
		missingDeadline atomic.Bool
	)

	unhealthyAddr.Store(&upstreamAddrs[1])

	lb := &loadbalancer.TCP{
		Logger: zaptest.NewLogger(suite.T()),
		HealthCheck: func(ctx context.Context, address string) error {
			if _, ok := ctx.Deadline(); !ok {
				missingDeadline.Store(true)
			}

			if addr := unhealthyAddr.Load(); addr != nil && *addr == address {
				return errors.New("not ready")
			}

			return loadbalancer.DialHealthCheck(ctx, address)
		},
	}
	suite.Require().NoError(lb.AddRoute(
		listenAddr,
		slices.Values(upstreamAddrs),
		upstream.WithLowHighScores(-3, 3),
		upstream.WithInitialScore(-1),
		upstream.WithScoreDeltas(-1, 1),
		upstream.WithHealthcheckInterval(10*time.Millisecond),
		upstream.WithHealthcheckTimeout(100*time.Millisecond),
		upstream.WithTiers(0, 0, 0),
	))

	suite.Require().NoError(lb.Start())

	var wg sync.WaitGroup

	wg.Go(func() {
		lb.Wait() //nolint: errcheck
	})

	t.Cleanup(func() {
		assert.NoError(t, lb.Close())
		wg.Wait()
	})

	// pickedUpstreams returns the number of times each upstream was picked for n connections.
	//
	// It is called from EventuallyWithT condition, so it reports failures without stopping the test goroutine.
	// The load balancer closes the connection without any data if no upstream is available, such connections are not counted.
	pickedUpstreams := func(t assert.TestingT, n int) map[string]int {
		picked := map[string]int{}

		for range n {
			c, err := (&net.Dialer{}).DialContext(suite.T().Context(), "tcp", listenAddr)
			if !assert.NoError(t, err) {
				return picked
			}

			id, err := io.ReadAll(c)
			assert.NoError(t, c.Close())

			if !assert.NoError(t, err) {
				return picked
			}

			if len(id) > 0 {
				picked[string(id)]++
			}
		}

		return picked
	}

	// initial score is -1, so wait for the healthy upstreams to pass the health check
	suite.Require().EventuallyWithT(func(collect *assert.CollectT) {
		assert.Equal(collect, map[string]int{"0": 2, "2": 2}, pickedUpstreams(collect, 4))
	}, 5*time.Second, 10*time.Millisecond)

	suite.Assert().Equal(map[string]int{"0": 10, "2": 10}, pickedUpstreams(t, 20))

	// upstream 1 passes the health check now
	unhealthyAddr.Store(nil)

	suite.Require().EventuallyWithT(func(collect *assert.CollectT) {
		assert.Len(collect, pickedUpstreams(collect, upstreamCount), upstreamCount)
	}, 5*time.Second, 10*time.Millisecond)

	// upstream 0 fails the health check now
	unhealthyAddr.Store(&upstreamAddrs[0])

	suite.Require().EventuallyWithT(func(collect *assert.CollectT) {
		picked := pickedUpstreams(collect, 2*upstreamCount)

		assert.NotContains(collect, picked, "0")
		assert.Len(collect, picked, 2)
	}, 5*time.Second, 10*time.Millisecond)

	suite.Assert().False(missingDeadline.Load(), "health check context should have a deadline")
}

func (suite *TCPSuite) TestTierThresholdsValidation() {
	for _, test := range []struct {
		name       string
		wantErr    string
		thresholds []time.Duration
	}{
		{name: "default", thresholds: nil},
		{name: "single", thresholds: []time.Duration{0}},
		{name: "custom", thresholds: []time.Duration{10 * time.Millisecond, 100 * time.Millisecond, time.Second}},
		{name: "empty", thresholds: []time.Duration{}, wantErr: "number of tier thresholds should be between 1 and 11, got 0"},
		{name: "too many", thresholds: make([]time.Duration, 12), wantErr: "number of tier thresholds should be between 1 and 11, got 12"},
		{name: "not increasing", thresholds: []time.Duration{0, time.Second, time.Second}, wantErr: "tier thresholds should be strictly increasing, got 1s after 1s"},
	} {
		suite.Run(test.name, func() {
			listenAddr, err := findListenAddress()
			suite.Require().NoError(err)

			lb := &loadbalancer.TCP{
				Logger:         zaptest.NewLogger(suite.T()),
				TierThresholds: test.thresholds,
			}

			err = lb.AddRoute(listenAddr, nil)

			if test.wantErr != "" {
				suite.Require().EqualError(err, test.wantErr)

				return
			}

			suite.Require().NoError(err)
			suite.Require().NoError(lb.Close())
		})
	}
}

func TestTCPSuite(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	suite.Run(t, new(TCPSuite))
}

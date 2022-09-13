// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package loadbalancer_test

import (
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

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

	lb := &loadbalancer.TCP{}
	suite.Require().NoError(lb.AddRoute(
		listenAddr,
		upstreamAddrs[:pivot],
		upstream.WithLowHighScores(-3, 3),
		upstream.WithInitialScore(1),
		upstream.WithScoreDeltas(-1, 1),
		upstream.WithHealthcheckInterval(time.Second),
		upstream.WithHealthcheckTimeout(100*time.Millisecond),
	))

	suite.Require().NoError(lb.Start())

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		lb.Wait() //nolint: errcheck
	}()

	for i := 0; i < 5*pivot; i++ {
		c, err := net.Dial("tcp", listenAddr)
		suite.Require().NoError(err)

		id, err := io.ReadAll(c)
		suite.Require().NoError(err)

		// load balancer should go round-robin across all the upstreams [0:pivot]
		suite.Assert().Equal([]byte(strconv.Itoa(i%pivot)), id)

		suite.Require().NoError(c.Close())
	}

	// reconcile the list
	suite.Require().NoError(lb.ReconcileRoute(listenAddr, upstreamAddrs[pivot:]))

	// bring down pre-pivot upstreams
	for i := 0; i < pivot; i++ {
		upstreams[i].Close()
	}

	upstreamsUsed := map[int64]int{}

	for i := 0; i < 10*(upstreamCount-pivot); i++ {
		c, err := net.Dial("tcp", listenAddr)
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

	lb := &loadbalancer.TCP{}
	suite.Require().NoError(lb.AddRoute(
		listenAddr,
		upstreamAddrs,
		upstream.WithLowHighScores(-3, 3),
		upstream.WithInitialScore(1),
		upstream.WithScoreDeltas(-1, 1),
		upstream.WithHealthcheckInterval(time.Second),
		upstream.WithHealthcheckTimeout(100*time.Millisecond),
	))

	suite.Require().NoError(lb.Start())

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		lb.Wait() //nolint: errcheck
	}()

	for i := 0; i < 2*upstreamCount; i++ {
		c, err := net.Dial("tcp", listenAddr)
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

	for i := 0; i < 10*upstreamCount; i++ {
		c, err := net.Dial("tcp", listenAddr)
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

func TestTCPSuite(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	suite.Run(t, new(TCPSuite))
}

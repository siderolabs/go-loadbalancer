// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package upstream_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/siderolabs/gen/xslices"
	"github.com/siderolabs/go-retry/retry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"

	"github.com/siderolabs/go-loadbalancer/upstream"
)

type mockBackend string

func (b mockBackend) HealthCheck(ctx context.Context) (upstream.Tier, error) {
	switch string(b) {
	case "fail":
		return -1, errors.New("fail")
	case "success":
		return -1, nil
	default:
		<-ctx.Done()

		return -1, ctx.Err()
	}
}

type ListSuite struct {
	suite.Suite
}

func (suite *ListSuite) TestEmpty() {
	l, err := upstream.NewList[mockBackend](nil)
	suite.Require().NoError(err)

	defer l.Shutdown()

	backend, err := l.Pick()
	suite.Assert().Zero(backend)
	suite.Assert().EqualError(err, "no upstreams available")
}

func (suite *ListSuite) TestRoundRobin() {
	l, err := upstream.NewList([]upstream.Backend{mockBackend("one"), mockBackend("two"), mockBackend("three")})
	suite.Require().NoError(err)

	defer l.Shutdown()

	backend, err := l.Pick()
	suite.Assert().Equal(mockBackend("one"), backend)
	suite.Assert().NoError(err)

	backend, err = l.Pick()
	suite.Assert().Equal(mockBackend("two"), backend)
	suite.Assert().NoError(err)

	backend, err = l.Pick()
	suite.Assert().Equal(mockBackend("three"), backend)
	suite.Assert().NoError(err)

	backend, err = l.Pick()
	suite.Assert().Equal(mockBackend("one"), backend)
	suite.Assert().NoError(err)
}

func (suite *ListSuite) TestDownUp() {
	l, err := upstream.NewList(
		[]mockBackend{"one", "two", "three"},
		upstream.WithLowHighScores(-3, 3),
		upstream.WithInitialScore(1),
		upstream.WithScoreDeltas(-1, 1),
		upstream.WithHealthcheckInterval(time.Hour),
	)
	suite.Require().NoError(err)

	defer l.Shutdown()

	backend, err := l.Pick()
	suite.Assert().Equal(mockBackend("one"), backend)
	suite.Assert().NoError(err)

	l.Down("two")   // score == 0
	l.Down("two")   // score == -1
	l.Down("three") // score == 0

	backend, err = l.Pick()
	suite.Assert().Equal(mockBackend("three"), backend)
	suite.Assert().NoError(err)

	backend, err = l.Pick()
	suite.Assert().Equal(mockBackend("one"), backend)
	suite.Assert().NoError(err)

	backend, err = l.Pick()
	suite.Assert().Equal(mockBackend("three"), backend)
	suite.Assert().NoError(err)

	l.Down("three") // score == -1
	l.Up("two")     // score == 0
	l.Up("two")     // score == 1
	l.Up("two")     // score == 2
	l.Up("two")     // score == 3
	l.Up("two")     // score == 3 (capped at highScore)

	backend, err = l.Pick()
	suite.Assert().Equal(mockBackend("one"), backend)
	suite.Assert().NoError(err)

	backend, err = l.Pick()
	suite.Assert().Equal(mockBackend("two"), backend)
	suite.Assert().NoError(err)

	backend, err = l.Pick()
	suite.Assert().Equal(mockBackend("one"), backend)
	suite.Assert().NoError(err)

	l.Down("two") // score == 2
	l.Down("two") // score == 1
	l.Down("two") // score == 0
	l.Down("two") // score == -1

	backend, err = l.Pick()
	suite.Assert().Equal(mockBackend("one"), backend)
	suite.Assert().NoError(err)

	l.Down("two") // score == -2
	l.Down("two") // score == -3
	l.Down("two") // score == -3 (capped at lowScore)

	backend, err = l.Pick()
	suite.Assert().Equal(mockBackend("one"), backend)
	suite.Assert().NoError(err)

	l.Up("two") // score == -2
	l.Up("two") // score == -1
	l.Up("two") // score == 0

	backend, err = l.Pick()
	suite.Assert().Equal(mockBackend("two"), backend)
	suite.Assert().NoError(err)
}

func (suite *ListSuite) TestHealthcheck() {
	l, err := upstream.NewList(
		[]mockBackend{"success", "fail", "timeout"},
		upstream.WithLowHighScores(-1, 1),
		upstream.WithInitialScore(1),
		upstream.WithScoreDeltas(-1, 1),
		upstream.WithHealthcheckInterval(10*time.Millisecond),
		upstream.WithHealthcheckTimeout(time.Millisecond),
	)
	suite.Require().NoError(err)

	defer l.Shutdown()

	time.Sleep(20 * time.Millisecond) // let healthchecks run

	// when health info converges, "success" should be the only backend left
	suite.Require().NoError(retry.Constant(time.Second, retry.WithUnits(time.Millisecond)).Retry(func() error {
		for range 10 {
			backend, err := l.Pick()
			if err != nil {
				return err
			}

			if backend != "success" {
				return retry.ExpectedError(fmt.Errorf("unexpected %v", backend))
			}
		}

		return nil
	}))
}

func (suite *ListSuite) TestReconcile() {
	l, err := upstream.NewList(
		[]mockBackend{"one", "two", "three"},
		upstream.WithLowHighScores(-3, 3),
		upstream.WithInitialScore(1),
		upstream.WithScoreDeltas(-1, 1),
		upstream.WithHealthcheckInterval(time.Hour),
	)
	suite.Require().NoError(err)

	defer l.Shutdown()

	backend, err := l.Pick()
	suite.Assert().Equal(mockBackend("one"), backend)
	suite.Assert().NoError(err)

	l.Reconcile([]mockBackend{"one", "two", "three"})

	backend, err = l.Pick()
	suite.Assert().Equal(mockBackend("two"), backend)
	suite.Assert().NoError(err)

	l.Reconcile([]mockBackend{"one", "two", "four"})

	backend, err = l.Pick()
	suite.Assert().Equal(mockBackend("four"), backend)
	suite.Assert().NoError(err)

	l.Reconcile([]mockBackend{"five", "six", "four"})

	backend, err = l.Pick()
	suite.Assert().Equal(mockBackend("four"), backend)
	suite.Assert().NoError(err)

	backend, err = l.Pick()
	suite.Assert().Equal(mockBackend("five"), backend)
	suite.Assert().NoError(err)

	backend, err = l.Pick()
	suite.Assert().Equal(mockBackend("six"), backend)
	suite.Assert().NoError(err)

	l.Down("four") // score == 2
	l.Down("four") // score == 1
	l.Down("four") // score == 0
	l.Down("four") // score == -1

	backend, err = l.Pick()
	suite.Assert().Equal(mockBackend("five"), backend)
	suite.Assert().NoError(err)

	backend, err = l.Pick()
	suite.Assert().Equal(mockBackend("six"), backend)
	suite.Assert().NoError(err)

	l.Reconcile([]mockBackend{"five", "six", "four"})

	backend, err = l.Pick()
	suite.Assert().Equal(mockBackend("five"), backend)
	suite.Assert().NoError(err)

	backend, err = l.Pick()
	suite.Assert().Equal(mockBackend("six"), backend)
	suite.Assert().NoError(err)

	l.Reconcile(nil)

	backend, err = l.Pick()
	suite.Assert().Zero(backend)
	suite.Assert().EqualError(err, "no upstreams available")
}

func (suite *ListSuite) TestBalancing() {
	var bc backendControl

	l, lerr := upstream.NewListWithCmp(
		[]*customBackend{
			bc.SetBackend("one", 1, nil),
			bc.SetBackend("two", 1, nil),
			bc.SetBackend("three", 1, nil),
		},
		func(a, b *customBackend) bool { return a.name == b.name },
		upstream.WithLowHighScores(-3, 3),
		upstream.WithInitialScore(1),
		upstream.WithScoreDeltas(-1, 1),
		upstream.WithHealthcheckInterval(10*time.Microsecond),
		upstream.WithHealthcheckTimeout(time.Hour),
		upstream.WithTiers(0, 5, 1),
	)
	suite.Require().NoError(lerr)

	defer l.Shutdown()

	// Move one to zero tier
	waitForUpdate(suite.T(), bc.SetBackend("one", 0, nil), 1)

	// Should always pick one
	for range 100 {
		backend, err := l.Pick()
		suite.Assert().NoError(err)
		suite.Assert().Equal("one", backend.name)
	}

	// Move one to second tier
	waitForUpdate(suite.T(), bc.SetBackend("one", 2, nil), 1)

	// Should always pick two or three
	seen := map[string]struct{}{}

	for range 100 {
		backend, err := l.Pick()
		suite.Assert().NoError(err)

		seen[backend.name] = struct{}{}
	}

	suite.Assert().Equal(xslices.ToSet([]string{"two", "three"}), seen)

	// Decrease score of two
	// score 1 --> -3
	waitForUpdate(suite.T(), bc.SetBackend("two", -1, errors.New("fail")), 4)

	// Should always pick three
	for range 100 {
		backend, err := l.Pick()
		suite.Assert().Equal("three", backend.name)
		suite.Assert().NoError(err)
	}

	// Decrease score of three
	waitForUpdate(suite.T(), bc.SetBackend("three", -1, errors.New("fail")), 4)

	// Should always pick one
	for range 100 {
		backend, err := l.Pick()
		suite.Assert().NoError(err)
		suite.Assert().Equal("one", backend.name)
	}

	// Decrease score of one
	waitForUpdate(suite.T(), bc.SetBackend("one", -1, errors.New("fail")), 4)

	// Should get nothing
	backend, err := l.Pick()
	suite.Assert().Zero(backend)
	suite.Assert().EqualError(err, "no upstreams available")

	// Increase score of two
	// score -3 --> 1
	waitForUpdate(suite.T(), bc.SetBackend("two", -1, nil), 4)

	// Should always pick two
	for range 100 {
		backend, err := l.Pick()
		suite.Assert().Equal("two", backend.name)
		suite.Assert().NoError(err)
	}

	// Increase score of three
	// score -3 --> 1
	waitForUpdate(suite.T(), bc.SetBackend("three", -1, nil), 4)

	// Should always pick two or three
	seen = map[string]struct{}{}

	for range 100 {
		backend, err := l.Pick()
		suite.Assert().NoError(err)

		seen[backend.name] = struct{}{}
	}

	suite.Assert().Equal(xslices.ToSet([]string{"two", "three"}), seen)

	// Move three to zero tier
	waitForUpdate(suite.T(), bc.SetBackend("three", 0, nil), 0)

	// Should always pick three
	for range 100 {
		backend, err := l.Pick()
		suite.Assert().Equal("three", backend.name)
		suite.Assert().NoError(err)
	}

	// Increase score of one
	// score -3 --> 1
	waitForUpdate(suite.T(), bc.SetBackend("one", -1, nil), 4)

	// Still should always pick three
	for range 100 {
		backend, err := l.Pick()
		suite.Assert().Equal("three", backend.name)
		suite.Assert().NoError(err)
	}

	// Move one to zero tier
	waitForUpdate(suite.T(), bc.SetBackend("one", 0, nil), 1)

	// Should always pick one or three
	seen = map[string]struct{}{}

	for range 100 {
		backend, err := l.Pick()
		suite.Assert().NoError(err)

		seen[backend.name] = struct{}{}
	}

	suite.Assert().Equal(xslices.ToSet([]string{"one", "three"}), seen)

	// Move two to zero tier
	waitForUpdate(suite.T(), bc.SetBackend("two", 0, nil), 1)

	// Should pick all three
	seen = map[string]struct{}{}

	for range 100 {
		backend, err := l.Pick()
		suite.Assert().NoError(err)

		seen[backend.name] = struct{}{}
	}

	suite.Assert().Equal(xslices.ToSet([]string{"one", "two", "three"}), seen)

	// Reconcile
	l.Reconcile([]*customBackend{
		bc.GetBackend("one"),
		bc.GetBackend("two"),
		bc.SetBackend("four", 1, nil),
	})

	// Should pick one or two
	seen = map[string]struct{}{}

	for range 100 {
		backend, err := l.Pick()
		suite.Assert().NoError(err)

		seen[backend.name] = struct{}{}
	}

	suite.Assert().Equal(xslices.ToSet([]string{"one", "two"}), seen)

	// Move one and two to first tier
	waitForUpdate(suite.T(), bc.SetBackend("one", 1, nil), 1)
	waitForUpdate(suite.T(), bc.SetBackend("two", 1, nil), 1)

	// Should pick all three
	seen = map[string]struct{}{}

	for range 100 {
		backend, err := l.Pick()
		suite.Assert().NoError(err)

		seen[backend.name] = struct{}{}
	}

	suite.Assert().Equal(xslices.ToSet([]string{"one", "two", "four"}), seen)

	// Reconcile
	l.Reconcile([]*customBackend{
		bc.GetBackend("one"),
		bc.SetBackend("five", 0, nil),
		bc.SetBackend("six", 0, nil),
	})

	// Should pick all three
	seen = map[string]struct{}{}

	for range 100 {
		backend, err := l.Pick()
		suite.Assert().NoError(err)

		seen[backend.name] = struct{}{}
	}

	suite.Assert().Equal(xslices.ToSet([]string{"one", "five", "six"}), seen)

	// Move one to second tier
	waitForUpdate(suite.T(), bc.SetBackend("one", 2, nil), 1)

	// Reconile
	l.Reconcile([]*customBackend{
		bc.GetBackend("one"),
		bc.SetBackend("seven", 1, nil),
		bc.SetBackend("eight", 1, nil),
	})

	// Should pick seven or eight
	seen = map[string]struct{}{}

	for range 100 {
		backend, err := l.Pick()
		suite.Assert().NoError(err)

		seen[backend.name] = struct{}{}
	}

	suite.Assert().Equal(xslices.ToSet([]string{"seven", "eight"}), seen)
}

func waitForUpdate(t *testing.T, backend *customBackend, times int64) {
	prev := backend.called.Load()

	assert.Eventually(t, func() bool { return backend.called.Load() >= prev+times }, time.Second, 20*time.Microsecond)
}

func TestListSuite(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	suite.Run(t, new(ListSuite))
}

type customBackend struct {
	ctrl   *backendControl
	name   string
	called atomic.Int64
}

func (b *customBackend) HealthCheck(context.Context) (upstream.Tier, error) {
	b.ctrl.RLock()
	defer b.ctrl.RUnlock()

	b.called.Add(1)

	data, ok := b.ctrl.Backends[b.name]
	if !ok {
		panic("unknown backend")
	}

	return data.Result.Tier, data.Result.Error
}

type backendResult struct {
	Error error
	Tier  upstream.Tier
}

//nolint:govet
type backendControl struct {
	sync.RWMutex
	Backends map[string]*struct {
		Result  backendResult
		Backend customBackend
	}
}

func (c *backendControl) GetBackend(name string) *customBackend {
	c.RLock()
	defer c.RUnlock()

	data, ok := c.Backends[name]
	if !ok {
		panic("unknown backend")
	}

	return &data.Backend
}

func (c *backendControl) SetBackend(name string, tier upstream.Tier, err error) *customBackend {
	c.Lock()
	defer c.Unlock()

	if c.Backends == nil {
		c.Backends = map[string]*struct {
			Result  backendResult
			Backend customBackend
		}{}
	}

	data := c.Backends[name]
	if data == nil {
		data = &struct {
			Result  backendResult
			Backend customBackend
		}{}

		c.Backends[name] = data

		data.Backend = customBackend{name: name, ctrl: c}
	}

	data.Result = backendResult{Error: err, Tier: tier}

	return &data.Backend
}

// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package upstream provides utilities for choosing upstream backends based on score.
package upstream

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/siderolabs/gen/xiter"
	"github.com/siderolabs/gen/xslices"
)

// ErrNoUpstreams is returned from Pick method, when there are no upstreams available.
var ErrNoUpstreams = fmt.Errorf("no upstreams available")

// Backend is an interface which should be implemented for a Pick entry.
type Backend interface {
	HealthCheck(ctx context.Context) (Tier, error)
}

// BackendCmp is an interface which should be implemented for a Pick entry.
// It is similar to [Backend], but used for generics.
type BackendCmp interface {
	comparable
	Backend
}

type node[T Backend] struct {
	backend T
	score   float64
	tier    Tier
}

// ListOption allows to configure List.
type ListOption func(*ListConfig) error

// WithLowHighScores configures low and high score.
func WithLowHighScores(lowScore, highScore float64) ListOption {
	return func(l *ListConfig) error {
		switch {
		case lowScore > 0:
			return fmt.Errorf("lowScore should be non-positive")
		case highScore < 0:
			return fmt.Errorf("highScore should be non-positive")
		case lowScore > highScore:
			return fmt.Errorf("lowScore should be less or equal to highScore")
		}

		l.lowScore, l.highScore = lowScore, highScore

		return nil
	}
}

// WithScoreDeltas configures fail and success score delta.
func WithScoreDeltas(failScoreDelta, successScoreDelta float64) ListOption {
	return func(l *ListConfig) error {
		switch {
		case failScoreDelta >= 0:
			return fmt.Errorf("failScoreDelta should be negative")
		case successScoreDelta <= 0:
			return fmt.Errorf("successScoreDelta should be positive")
		}

		l.failScoreDelta, l.successScoreDelta = failScoreDelta, successScoreDelta

		return nil
	}
}

// WithInitialScore configures initial backend score.
func WithInitialScore(initialScore float64) ListOption {
	return func(l *ListConfig) error {
		l.initialScore = initialScore

		return nil
	}
}

// WithHealthcheckInterval configures healthcheck interval.
func WithHealthcheckInterval(interval time.Duration) ListOption {
	return func(l *ListConfig) error {
		l.healthcheckInterval = interval

		return nil
	}
}

// WithHealthCheckJitter configures healthcheck jitter (0.0 - 1.0).
func WithHealthCheckJitter(jitter float64) ListOption {
	return func(l *ListConfig) error {
		if jitter < 0 || jitter > 1 {
			return fmt.Errorf("healthcheck jitter should in range [0, 1]: %f", jitter)
		}

		l.healthcheckJitter = jitter

		return nil
	}
}

// WithHealthcheckTimeout configures healthcheck timeout (for each backend).
func WithHealthcheckTimeout(timeout time.Duration) ListOption {
	return func(l *ListConfig) error {
		l.healthcheckTimeout = timeout

		return nil
	}
}

// Tier is a type for backend tier.
type Tier int

// WithTiers configures backend tier min, max, and start.
func WithTiers(minTier, maxTier, initTier Tier) ListOption {
	return func(l *ListConfig) error {
		switch {
		case minTier < 0 || maxTier < 0 || initTier < 0:
			return errors.New("min, max and init tiers should be non-negative")
		case minTier > maxTier:
			return errors.New("min tier should be less or equal to max tier")
		case initTier < minTier || initTier > maxTier:
			return errors.New("init tier should be between min and max tier")
		case minTier > 10 || maxTier > 10 || initTier > 10:
			return errors.New("min, max and init tiers should be less or equal to 10")
		}

		l.initTier, l.minTier, l.maxTier = initTier, minTier, maxTier

		return nil
	}
}

// List of upstream Backends with healthchecks and different strategies to pick a node.
//
// List keeps track of Backends with score. Score is updated on health checks, and via external
// interface (e.g. when actual connection fails).
//
// Initial score is set via options (default is +1). Low and high scores defaults are (-3, +3).
// Backend score is limited by low and high scores. Each time healthcheck fails score is adjusted
// by fail delta score, and every successful check updates score by success score delta (defaults are -1/+1).
//
// Backend might be used if its score is not negative.
type List[T Backend] struct { //nolint:govet
	listConfig

	cmp func(T, T) bool

	// Following fields are protected by mutex
	mu      sync.Mutex
	nodes   []node[T]
	current int
}

// ListConfig is a configuration for List. It is separated from List to allow
// usage of functional options without exposing type in their API.
type ListConfig struct { //nolint:govet
	healthcheckInterval time.Duration
	healthcheckTimeout  time.Duration

	healthWg        sync.WaitGroup
	healthCtxCancel context.CancelFunc

	lowScore, highScore               float64
	failScoreDelta, successScoreDelta float64
	initialScore                      float64
	healthcheckJitter                 float64

	initialHealthcheckDoneCh chan struct{}

	minTier, maxTier, initTier Tier
}

// This allows us to hide embedded struct from public access.
type listConfig = ListConfig

// NewList initializes new list with upstream backends and options and starts health checks. It uses
//
// List should be stopped with `.Shutdown()`.
func NewList[T BackendCmp](upstreams iter.Seq[T], options ...ListOption) (*List[T], error) {
	return NewListWithCmp[T](upstreams, func(a, b T) bool { return a == b }, options...)
}

// NewListWithCmp initializes new list with upstream backends and options and starts health checks.
//
// List should be stopped with `.Shutdown()`.
func NewListWithCmp[T Backend](upstreams iter.Seq[T], cmp func(T, T) bool, options ...ListOption) (*List[T], error) {
	// initialize with defaults
	list := &List[T]{
		listConfig: listConfig{
			lowScore:          -3.0,
			highScore:         3.0,
			failScoreDelta:    -1.0,
			successScoreDelta: 1.0,
			initialScore:      1.0,

			healthcheckInterval: 1 * time.Second,
			healthcheckTimeout:  100 * time.Millisecond,
			minTier:             0,
			maxTier:             4,
			initTier:            0,

			initialHealthcheckDoneCh: make(chan struct{}),
		},

		cmp:     cmp,
		current: -1,
	}

	var ctx context.Context

	ctx, list.healthCtxCancel = context.WithCancel(context.Background())

	for _, opt := range options {
		if err := opt(&list.listConfig); err != nil {
			return nil, err
		}
	}

	if upstreams == nil {
		upstreams = xiter.Empty[T]
	}

	list.nodes = slices.Collect(xiter.Map(func(b T) node[T] {
		return node[T]{
			backend: b,
			score:   list.initialScore,
			tier:    list.initTier,
		}
	}, upstreams))

	list.healthWg.Add(1)

	go list.healthcheck(ctx)

	return list, nil
}

// Reconcile the list of backends with passed list.
//
// Any new backends are added with initial score, score is untouched
// for backends which haven't changed their score.
func (list *List[T]) Reconcile(toAdd iter.Seq[T]) {
	list.mu.Lock()
	defer list.mu.Unlock()

	if toAdd == nil {
		toAdd = xiter.Empty[T]
	}

	list.nodes = xslices.FilterInPlace(list.nodes, func(b node[T]) bool {
		_, ok := xiter.Find(func(u T) bool { return list.cmp(u, b.backend) }, toAdd)

		return ok // if not ok, backend doesn't exist in new upstreams, remove from current node list
	})

	for newB := range toAdd {
		if slices.ContainsFunc(list.nodes, func(b node[T]) bool { return list.cmp(newB, b.backend) }) {
			// if backend exists, remove from toAdd slice, preserve in current node list
			continue
		}

		list.nodes = append(list.nodes, node[T]{
			backend: newB,
			score:   list.initialScore,
			tier:    list.initTier,
		})
	}
}

// Shutdown stops healthchecks.
func (list *List[T]) Shutdown() {
	list.healthCtxCancel()

	list.healthWg.Wait()
}

// Up increases backend score by success score delta.
func (list *List[T]) Up(upstream T) {
	list.upWithTier(upstream, -1)
}

func (list *List[T]) upWithTier(upstream T, newTier Tier) {
	list.mu.Lock()
	defer list.mu.Unlock()

	for i := range list.nodes {
		if list.cmp(list.nodes[i].backend, upstream) {
			list.nodes[i].score += list.successScoreDelta
			list.updateNodeTier(i, newTier)

			if list.nodes[i].score > list.highScore {
				list.nodes[i].score = list.highScore
			}
		}
	}
}

func (list *List[T]) updateNodeTier(i int, newTier Tier) {
	switch {
	case newTier == -1:
		// do nothing, keep old tier
		return
	case newTier < list.minTier:
		newTier = list.minTier
	case newTier > list.maxTier:
		newTier = list.maxTier
	}

	list.nodes[i].tier = newTier
}

// Down decreases backend score by fail score delta.
func (list *List[T]) Down(upstream T) {
	list.downWithTier(upstream, -1)
}

func (list *List[T]) downWithTier(upstream T, newTier Tier) {
	list.mu.Lock()
	defer list.mu.Unlock()

	for i := range list.nodes {
		if list.cmp(list.nodes[i].backend, upstream) {
			list.nodes[i].score += list.failScoreDelta
			list.updateNodeTier(i, newTier)

			if list.nodes[i].score < list.lowScore {
				list.nodes[i].score = list.lowScore
			}
		}
	}
}

// WaitForInitialHealthcheck waits for initial healthcheck to be completed.
func (list *List[T]) WaitForInitialHealthcheck(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-list.initialHealthcheckDoneCh:
		return nil
	}
}

// Pick returns next backend to be used.
//
// Default policy is to pick healthy (non-negative score) backend in
// round-robin fashion.
func (list *List[T]) Pick() (T, error) { //nolint:ireturn
	list.mu.Lock()
	defer list.mu.Unlock()

	nodes := list.nodes

	for tier := list.minTier; tier <= list.maxTier; tier++ {
		for j := range nodes {
			i := (list.current + 1 + j) % len(nodes)

			if nodes[i].tier == tier && nodes[i].score >= 0 {
				list.current = i

				return nodes[list.current].backend, nil
			}
		}
	}

	var zero T

	return zero, ErrNoUpstreams
}

func (list *List[T]) healthcheck(ctx context.Context) {
	defer list.healthWg.Done()

	// run the initial health check immediately
	list.doHealthCheck(ctx)

	close(list.initialHealthcheckDoneCh)

	initialInterval := list.healthcheckInterval
	if list.healthcheckJitter > 0 {
		// jitter is enabled - stagger the second health check by setting the first wait time to a random duration between 0 and the full interval
		initialInterval = time.Duration(rand.Float64() * float64(list.healthcheckInterval))
	}

	timer := time.NewTimer(initialInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		list.doHealthCheck(ctx)

		nextInterval := time.Duration(((rand.Float64()*2-1)*list.healthcheckJitter + 1.0) * float64(list.healthcheckInterval))

		timer.Reset(nextInterval)
	}
}

func (list *List[T]) doHealthCheck(ctx context.Context) {
	list.mu.Lock()
	backends := xslices.Map(list.nodes, func(n node[T]) T { return n.backend })
	list.mu.Unlock()

	for _, backend := range backends {
		if ctx.Err() != nil {
			return
		}

		func() {
			localCtx, ctxCancel := context.WithTimeout(ctx, list.healthcheckTimeout)
			defer ctxCancel()

			if newTier, err := backend.HealthCheck(localCtx); err != nil {
				list.downWithTier(backend, newTier)
			} else {
				list.upWithTier(backend, newTier)
			}
		}()
	}
}

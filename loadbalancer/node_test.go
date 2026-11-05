// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package loadbalancer_test

import (
	"testing"
	"time"

	"github.com/siderolabs/go-loadbalancer/loadbalancer"
	"github.com/siderolabs/go-loadbalancer/upstream"
)

func Test_calcTier(t *testing.T) {
	t.Parallel()

	customThresholds := []time.Duration{10 * time.Millisecond, 100 * time.Millisecond, time.Second}

	tests := map[string]struct {
		thresholds []time.Duration
		want       upstream.Tier
		elapsed    time.Duration
	}{
		"Microsecond":      {thresholds: loadbalancer.DefaultTierThresholds, want: upstream.Tier(0), elapsed: time.Microsecond},
		"100 Microseconds": {thresholds: loadbalancer.DefaultTierThresholds, want: upstream.Tier(1), elapsed: 100 * time.Microsecond},
		"Millisecond":      {thresholds: loadbalancer.DefaultTierThresholds, want: upstream.Tier(2), elapsed: time.Millisecond},
		"10 Milliseconds":  {thresholds: loadbalancer.DefaultTierThresholds, want: upstream.Tier(3), elapsed: 10 * time.Millisecond},
		"100 Milliseconds": {thresholds: loadbalancer.DefaultTierThresholds, want: upstream.Tier(4), elapsed: 100 * time.Millisecond},
		"1 Second":         {thresholds: loadbalancer.DefaultTierThresholds, want: upstream.Tier(5), elapsed: time.Second},

		"custom below first":      {thresholds: customThresholds, want: upstream.Tier(0), elapsed: time.Millisecond},
		"custom 10 Milliseconds":  {thresholds: customThresholds, want: upstream.Tier(0), elapsed: 10 * time.Millisecond},
		"custom 50 Milliseconds":  {thresholds: customThresholds, want: upstream.Tier(0), elapsed: 50 * time.Millisecond},
		"custom 100 Milliseconds": {thresholds: customThresholds, want: upstream.Tier(1), elapsed: 100 * time.Millisecond},
		"custom 5 Seconds":        {thresholds: customThresholds, want: upstream.Tier(2), elapsed: 5 * time.Second},

		"single threshold": {thresholds: []time.Duration{0}, want: upstream.Tier(0), elapsed: time.Second},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := loadbalancer.CalcTier(tt.thresholds, nil, tt.elapsed)
			if err != nil {
				t.Errorf("calcTier() error = %v", err)

				return
			}

			if got != tt.want {
				t.Errorf("calcTier() got = %v, want %v", got, tt.want)
			}
		})
	}
}

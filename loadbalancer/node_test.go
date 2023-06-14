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

	tests := map[string]struct {
		want    upstream.Tier
		elapsed time.Duration
	}{
		"Microsecond":      {want: upstream.Tier(0), elapsed: time.Microsecond},
		"100 Microseconds": {want: upstream.Tier(1), elapsed: 100 * time.Microsecond},
		"Millisecond":      {want: upstream.Tier(2), elapsed: time.Millisecond},
		"10 Milliseconds":  {want: upstream.Tier(3), elapsed: 10 * time.Millisecond},
		"100 Milliseconds": {want: upstream.Tier(4), elapsed: 100 * time.Millisecond},
		"1 Second":         {want: upstream.Tier(5), elapsed: time.Second},
	}
	for name, tt := range tests {
		tt := tt

		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, err := loadbalancer.CalcTier(nil, tt.elapsed)
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

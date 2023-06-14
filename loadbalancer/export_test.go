// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package loadbalancer

import (
	"time"

	"github.com/siderolabs/go-loadbalancer/upstream"
)

func CalcTier(err error, elapsed time.Duration) (upstream.Tier, error) { return calcTier(err, elapsed) }

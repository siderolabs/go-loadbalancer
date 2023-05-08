// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build !windows

package loadbalancer

import (
	"net"
	"syscall"
)

func probeDialer() *net.Dialer {
	return &net.Dialer{
		// The dialer reduces the TIME-WAIT period to 1 seconds instead of the OS default of 60 seconds.
		Control: func(network, address string, c syscall.RawConn) error {
			return c.Control(func(fd uintptr) {
				syscall.SetsockoptLinger(int(fd), syscall.SOL_SOCKET, syscall.SO_LINGER, &syscall.Linger{Onoff: 1, Linger: 1}) //nolint: errcheck
			})
		},
	}
}

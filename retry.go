/*
   Copyright 2020 Joseph Cumines

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package bigbuff

import (
	"context"
	"errors"
	"math/rand"
	"time"
)

const (
	defaultExponentialRetryRate = time.Millisecond * 300
	maxShiftUint32              = 31
)

// ExponentialRetry implements a simple exponential back off and retry, via closure wrapper, as described on Wikipedia
// (https://en.wikipedia.org/wiki/Exponential_backoff) supporting context canceling (while waiting / before starting),
// configurable base rate / slot time which will default to 300ms if rate is <= 0 (SUBJECT TO CHANGE), and the ability
// to support fatal errors via use of the FatalError error wrapper function provided by this package.
// Notes: 1. This function will panic if value is nil, but NOT if ctx is nil (the latter is not recommended but the
// existing implementations have this behavior already). 2. The exit case triggered via use of the FatalError wrapper
// will include any accompanying result, as well as the unpacked error value (which will always be non-nil since
// FatalError will panic otherwise). 3. Before each call to value the context error will be checked, and if non-nil
// will be propagated as-is with a nil result. 4. This implementation uses the math/rand package.
func ExponentialRetry(ctx context.Context, rate time.Duration, value func() (interface{}, error)) func() (interface{}, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if rate <= 0 {
		rate = defaultExponentialRetryRate
	}
	if value == nil {
		panic(errors.New("bigbuff.ExponentialRetry nil value"))
	}
	return func() (interface{}, error) {
		var c uint32
		for {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if c < maxShiftUint32 {
				c++
			}
			if result, err := value(); err == nil {
				return result, nil
			} else if isFatalError(err) {
				return result, unpackFatalError(err)
			}
			waitDuration(ctx, calcExponentialRetry(rate, c))
		}
	}
}

var waitDuration = func(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

var calcExponentialRetry = func(d time.Duration, c uint32) time.Duration {
	if c > maxShiftUint32 {
		c = maxShiftUint32
	}
	c = 1 << c
	return time.Duration(rand.Int63n(int64(c))) * d
}

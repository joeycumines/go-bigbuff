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
	"time"
)

// LinearAttempt returns a new channel that will be published, at most, every rate, for a maximum total of count
// messages, and will be closed after either reaching count or context cancel, whichever comes first. Note that it
// is buffered and will start with a single value, and behaves identically to time.NewTicker in that it will attempt
// to keep a constant rate, but compensates for slow consumers, and in that the value received will be the time at which
// the last attempt was scheduled (the offset from the current time being equivalent to the conflation rate). Either
// rate or count being <= 0 or ctx being nil will trigger a panic. Note that the initial publish will happen inline,
// and context errors are guarded, meaning if the context returns an error when first checked then the returned channel
// will always be closed, with no values sent.
//
// This implementation is designed to be iterated over, by using range, with resource freeing via context cancel.
// It is very useful when implementing something that will attempt to perform an action at a maximum rate, for a maximum
// amount of times, e.g. for linear retry logic.
func LinearAttempt(ctx context.Context, rate time.Duration, count int) <-chan time.Time {
	if ctx == nil || rate <= 0 || count <= 0 {
		panic(errors.New(`bigbuff.LinearAttempt invalid input`))
	}
	c := make(chan time.Time, 1)
	if ctx.Err() != nil {
		close(c)
		return c
	}
	c <- time.Now()
	count--
	if count <= 0 {
		close(c)
		return c
	}
	go func() {
		defer close(c)
		ticker := time.NewTicker(rate)
		defer ticker.Stop()
		for i := 0; i < count; {
			var t time.Time
			select {
			case <-ctx.Done():
				return
			case t = <-ticker.C:
			}
			if ctx.Err() != nil {
				// guarantee at most one tick after context cancel
				return
			}
			select {
			case c <- t:
				i++
			default:
				// slow consumer, retry send next tick
			}
		}
	}()
	return c
}

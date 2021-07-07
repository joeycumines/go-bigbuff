/*
   Copyright 2021 Joseph Cumines

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
	"fmt"
	"sync"
	"time"
)

var (
	errResolveNotCalled = errors.New(`bigbuff.Exclusive resolve not called`)
)

// ExclusiveKey configures a comparable value for grouping calls, for debouncing, limiting, etc.
func ExclusiveKey(value interface{}) ExclusiveOption {
	return func(c *exclusiveConfig) { c.key = value }
}

// ExclusiveWork configures the work function (what will actually get called).
func ExclusiveWork(value WorkFunc) ExclusiveOption {
	return func(c *exclusiveConfig) { c.work = value }
}

// ExclusiveValue implements a simpler style of ExclusiveWork (that was originally the only supported behavior).
func ExclusiveValue(value func() (interface{}, error)) ExclusiveOption {
	var work WorkFunc
	if value != nil {
		work = func(resolve func(result interface{}, err error)) { resolve(value()) }
	}
	return ExclusiveWork(work)
}

// ExclusiveWait configures the duration (since the start of the call) that should be waited, before actually calling
// the work function, see also Exclusive.CallAfter.
func ExclusiveWait(value time.Duration) ExclusiveOption {
	return func(c *exclusiveConfig) { c.wait = value }
}

// ExclusiveStart configures "start" behavior for the call, which, if true, will avoid the overhead required to
// propagate results, which will also cause the return value (outcome channel) to be nil. See also Exclusive.Start.
func ExclusiveStart(value bool) ExclusiveOption {
	return func(c *exclusiveConfig) { c.start = value }
}

// ExclusiveWrapper facilitates programmatic building of work, note that the ExclusiveWork or ExclusiveValue option
// must still be provided, but may be provided in any order (after or before this option). If there are multiple
// wrappers, they will be applied sequentially (left -> right is inner -> outer).
func ExclusiveWrapper(value func(value WorkFunc) WorkFunc) ExclusiveOption {
	return func(c *exclusiveConfig) { c.wrappers = append(c.wrappers, value) }
}

// ExclusiveRateLimit is typically a drop-in replacement for MinDuration that works properly with non-start calls, and
// returns a ExclusiveWrapper option. Note that the context is for cleaning up the resources required to apply the rate
// limit (the rate limit itself), and will also be used to guard the actual work, for safety reasons.
func ExclusiveRateLimit(ctx context.Context, minDuration time.Duration) ExclusiveOption {
	if ctx == nil {
		panic(fmt.Errorf("bigbuff.ExclusiveRateLimit nil context"))
	}
	if minDuration <= 0 {
		panic(fmt.Errorf("bigbuff.ExclusiveRateLimit invalid duration: %s", minDuration))
	}
	return ExclusiveWrapper(func(value WorkFunc) WorkFunc {
		if value == nil {
			panic(fmt.Errorf("bigbuff.ExclusiveRateLimit nil work func"))
		}
		return func(resolve func(result interface{}, err error)) {
			if err := ctx.Err(); err != nil {
				resolve(nil, err)
				return
			}
			ts := time.Now()
			value(resolve)
			if remainingDuration := ts.Add(minDuration).Sub(time.Now()); remainingDuration > 0 {
				timer := time.NewTimer(remainingDuration)
				defer timer.Stop()
				select {
				case <-ctx.Done():
				case <-timer.C:
				}
			}
		}
	})
}

// CallWithOptions consolidates the various different ways to use Exclusive into a single method, to improve
// maintainability w/o breaking API compatibility. Other methods such as Call and Start will continue to be supported.
func (e *Exclusive) CallWithOptions(options ...ExclusiveOption) <-chan *ExclusiveOutcome {
	var config exclusiveConfig
	for _, option := range options {
		option(&config)
	}
	for _, wrapper := range config.wrappers {
		config.work = wrapper(config.work)
	}
	config.wrappers = nil
	return e.call(config)
}

// Call uses a given key to ensure that the operation that the value callback represents will not be performed
// concurrently, and in the event that one or more operations are attempted while a given operation is still being
// performed, these operations will be grouped such that they are debounced to a single call, sharing the output.
//
// Note that this method will panic if the receiver is nil, or the value is nil, but a nil key is allowed.
func (e *Exclusive) Call(key interface{}, value func() (interface{}, error)) (interface{}, error) {
	return e.CallAfter(key, value, 0)
}

// CallAfter performs exactly the same operation as the Exclusive.Call method, but with an added wait to allow
// operations sent through in close succession to be grouped together, note that if wait is <= 0 it will be ignored.
func (e *Exclusive) CallAfter(key interface{}, value func() (interface{}, error), wait time.Duration) (interface{}, error) {
	v := <-e.CallAfterAsync(key, value, wait)
	return v.Result, v.Error
}

// CallAsync behaves exactly the same as Call but guarantees order (the value func) for synchronous calls.
//
// Note that the return value will always be closed after being sent the result, and will therefore any additional
// reads will always receive nil.
func (e *Exclusive) CallAsync(key interface{}, value func() (interface{}, error)) <-chan *ExclusiveOutcome {
	return e.CallAfterAsync(key, value, 0)
}

// CallAfterAsync behaves exactly the same as CallAfter but guarantees order (the value func) for synchronous calls.
//
// Note that the return value will always be closed after being sent the result, and will therefore any additional
// reads will always receive nil.
func (e *Exclusive) CallAfterAsync(key interface{}, value func() (interface{}, error), wait time.Duration) <-chan *ExclusiveOutcome {
	return e.CallWithOptions(ExclusiveKey(key), ExclusiveValue(value), ExclusiveWait(wait))
}

// Start is synonymous with a CallAsync that avoids spawning a unnecessary goroutines to wait for results
func (e *Exclusive) Start(key interface{}, value func() (interface{}, error)) {
	e.StartAfter(key, value, 0)
}

// StartAfter is synonymous with a CallAfterAsync that avoids spawning a unnecessary goroutines to wait for results
func (e *Exclusive) StartAfter(key interface{}, value func() (interface{}, error), wait time.Duration) {
	e.CallWithOptions(ExclusiveKey(key), ExclusiveValue(value), ExclusiveWait(wait), ExclusiveStart(true))
}

func (e *Exclusive) call(c exclusiveConfig) <-chan *ExclusiveOutcome {
	if e == nil || c.work == nil {
		panic(errors.New("bigbuff.Exclusive receiver and value must be non-nil"))
	}

	// find or create item
	var item *exclusiveItem
	for {
		// init map and obtain item
		e.mutex.Lock()
		if e.work == nil {
			e.work = make(map[interface{}]*exclusiveItem)
		}
		if _, ok := e.work[c.key]; !ok {
			var mu sync.Mutex
			e.work[c.key] = &exclusiveItem{
				mutex: &mu,
				cond:  sync.NewCond(&mu),
			}
		}
		item = e.work[c.key]
		e.mutex.Unlock()

		// lock item then the root to check if item is still valid
		item.mutex.Lock()

		var valid bool

		// check validity of item and initialise if so
		e.mutex.Lock()
		if v, _ := e.work[c.key]; v == item {
			valid = true

			// start waiting (for the purposes of the wait option)
			if item.ts == (time.Time{}) {
				item.ts = time.Now()
			}

			item.work = c.work
			item.wait = c.wait

			// WARNING see how this is used in the outcome handling
			item.count++

			// escape hatch for start case (avoids unnecessary contention)
			if c.start && item.count != 1 {
				e.mutex.Unlock()
				item.mutex.Unlock()
				return nil
			}
		}
		e.mutex.Unlock()

		if valid {
			// keep the item lock to prevent it from being no longer valid
			// (we remove from the map after obtaining item > root, in the same way)
			break
		}

		item.mutex.Unlock()
	}

	// all cases except "start" (no wait on outcome) return a non-nil channel
	var outcome chan *ExclusiveOutcome
	if !c.start {
		outcome = make(chan *ExclusiveOutcome, 1)
	}

	go func() {
		// wait until not running, which has two cases
		// 1) newly initialised item or item initialised while running
		// 2) item has been completed (by another waiter in the same batch)
		for item.running {
			item.cond.Wait()
		}

		if item.complete {
			// case 2)
			if outcome != nil {
				outcome <- &ExclusiveOutcome{
					Result: item.result,
					Error:  item.err,
				}
				close(outcome)
			}
			item.mutex.Unlock()
			return
		}

		// case 1)

		// the item is now running
		item.running = true

		// before we remove item from the work map, handle any specified wait, unlocking while we are waiting
		// so that other calls may register themselves on the item
		if item.wait > 0 {
			// adjust the sleep by how long we have already waited
			if wait := item.wait - time.Since(item.ts); wait > 0 {
				item.mutex.Unlock()
				time.Sleep(wait)
				item.mutex.Lock()
			}
		}

		// replace the item in the work map with a new one sharing the same mutex and cond and also running
		// (we still use our current item, but we only want calls started BEFORE this one to share the same result)
		e.mutex.Lock()
		nextItem := &exclusiveItem{
			mutex:   item.mutex,
			cond:    item.cond,
			running: true,
		}
		e.work[c.key] = nextItem
		e.mutex.Unlock()

		// release the mutex while we do the work
		item.mutex.Unlock()

		// call the work function, guaranteeing resolve, and blocking until work is complete
		{
			var (
				once    sync.Once
				resolve = func(result interface{}, err error) {
					once.Do(func() {
						if outcome != nil {
							outcome <- &ExclusiveOutcome{
								Result: result,
								Error:  err,
							}
							close(outcome)
						}
						item.mutex.Lock()
						item.result = result
						item.err = err
						item.complete = true
						item.running = false
						item.cond.Broadcast()
						item.mutex.Unlock()
					})
				}
			)
			item.work(resolve)
			resolve(nil, errResolveNotCalled)
		}

		// note this is the same mutex
		// the reason why we don't just do this as part of resolve is to allow the work func to apply limiting
		// (setting nextItem.running to false is what actually triggers the next job, if any)
		nextItem.mutex.Lock()
		nextItem.running = false
		if nextItem.count == 0 {
			e.mutex.Lock()
			delete(e.work, c.key)
			e.mutex.Unlock()
		}
		nextItem.cond.Broadcast()
		nextItem.mutex.Unlock()
	}()

	return outcome
}

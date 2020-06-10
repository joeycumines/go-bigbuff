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
	"fmt"
	"sync"
	"time"
)

func (b *Buffer) Close() (err error) {
	b.ensure()

	err = errors.New("bigbuff.Buffer.Close may only be called once")

	b.close.Do(func() {
		err = nil

		// write lock because it's for the cond (we need at least read for getting done + cancel)
		b.mutex.Lock()
		defer b.mutex.Unlock()

		// all resources should be freed after this call - we will close the done channel
		defer close(b.done)

		// cancel the context, so that the consumers will close themselves (therefore self-removing)
		// also ensures that Put and NewConsumer calls on a closing buffer will fail
		b.cancel()

		// block until all consumers are closed (they remove themselves from the internal mapping)
		for len(b.consumers) != 0 {
			b.cond.Wait()
		}
	})

	return
}

func (b *Buffer) Done() <-chan struct{} {
	b.ensure()

	b.mutex.RLock()
	defer b.mutex.RUnlock()

	return b.done
}

func (b *Buffer) Put(ctx context.Context, values ...interface{}) error {
	b.ensure()

	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()

	if err := b.ctx.Err(); err != nil {
		return err
	}

	b.buffer = append(b.buffer, values...)
	b.cond.Broadcast()

	return nil
}

// NewConsumer constructs a new consumer instance.
func (b *Buffer) NewConsumer() (Consumer, error) {
	b.ensure()

	b.mutex.Lock()
	defer b.mutex.Unlock()

	if err := b.ctx.Err(); err != nil {
		return nil, err
	}

	c := &consumer{
		done:     make(chan struct{}),
		producer: b,
	}

	c.cond = sync.NewCond(&c.mutex)

	c.ctx, c.cancel = context.WithCancel(b.ctx)

	go func() {
		// automatically close the consumer when the context is cancelled
		defer c.Close()
		<-c.ctx.Done()
	}()

	b.consumers[c] = b.offset // the consumer's initial offset becomes the start of the buffer
	b.cond.Broadcast()

	return c, nil
}

// Slice returns a copy of the internal message buffer (all currently stored in memory).
// NOTE: this will read-lock the buffer itself, and is accessible even after the buffer is closed.
func (b *Buffer) Slice() []interface{} {
	b.ensure()

	b.mutex.RLock()
	defer b.mutex.RUnlock()

	if b.buffer == nil {
		return nil
	}

	result := make([]interface{}, len(b.buffer))
	copy(result, b.buffer)

	return result
}

// Size returns the length of the buffer
func (b *Buffer) Size() int {
	b.ensure()

	b.mutex.RLock()
	defer b.mutex.RUnlock()

	return len(b.buffer)
}

// CleanerConfig returns the current cleaner config (which has defaults)
func (b *Buffer) CleanerConfig() CleanerConfig {
	b.ensure()

	b.mutex.RLock()
	defer b.mutex.RUnlock()

	return *b.cleaner
}

// SetCleanerConfig updates the cleaner config, returning an error if the config was invalid.
func (b *Buffer) SetCleanerConfig(config CleanerConfig) error {
	b.ensure()

	if config.Cleaner == nil {
		return errors.New("bigbuff.Buffer.SetCleanerConfig requires a non-nil config.Cleaner")
	}

	if config.Cooldown < 0 {
		return fmt.Errorf("bigbuff.Buffer.SetCleanerConfig negative config.Cooldown: %d", config.Cooldown)
	}

	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.cleaner = &config

	return nil
}

// Diff is provided to facilitate ranging over a buffer via a consumer, and returns the items remaining
// in the buffer (includes uncommitted), be aware that the value CAN be negative, in the event the consumer fell
// behind (the cleaner cleared item(s) from the buffer that the consumer hadn't read yet, which by default will never
// happen, as the default mode is a unbounded buffer).
// Note it will return (0, false) for any invalid consumers or any not registered on the receiver buffer.
func (b *Buffer) Diff(c Consumer) (int, bool) {
	b.ensure()

	// we need to get the actual consumer type (it's used as a map key)
	cm, ok := c.(*consumer)

	if !ok || cm == nil || cm.producer != b {
		return 0, false
	}

	// the consumer is always locked first
	cm.mutex.Lock()
	defer cm.mutex.Unlock()

	// then the buffer itself (we only need a read lock)
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	// now we can check to see if it was actually from this buffer
	offset, ok := b.consumers[cm]

	if !ok {
		return 0, false
	}

	// atomically checks the size of the buffer - the relative offset of the consumer
	return len(b.buffer) - (offset + cm.offset - b.offset), true
}

// Range provides a way to iterate from the start to the end of the buffer, note that it will exit as soon as it
// reaches the end of the buffer (unlike ranging on a channel), it simply utilizes the package Range + Buffer.Diff.
func (b *Buffer) Range(ctx context.Context, c Consumer, fn func(index int, value interface{}) bool) error {
	b.ensure()

	if cm, ok := c.(*consumer); !ok || cm == nil || cm.producer != b {
		return errors.New("bigbuff.Buffer.Range consumer must be one constructed via bigbuff.Buffer.NewConsumer")
	}

	if fn == nil {
		return errors.New("bigbuff.Buffer.Range nil fn")
	}

	if diff, ok := b.Diff(c); !ok || diff <= 0 {
		return nil
	}

	return Range(
		ctx,
		c,
		func(index int, value interface{}) bool {
			if !fn(index, value) {
				return false
			}
			diff, ok := b.Diff(c)
			return ok && diff > 0
		},
	)
}

// UNEXPORTED

// delete runs delete on the consumer map, for a given consumer, using the mutex and broadcasting
func (b *Buffer) delete(c *consumer) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// remove the consumer if it is part of the buffer
	delete(b.consumers, c)
	// we (may have) modified the buffer, broadcast it
	b.cond.Broadcast()
}

// commit applies a given offset modifier to a given consumer, returning an error if the consumer does not exist
func (b *Buffer) commit(c *consumer, offset int) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// retrieve the stored offset for the consumer
	if cOffset, ok := b.consumers[c]; !ok {
		return fmt.Errorf("bigbuff.Buffer.commit unknown consumer: %p", c)
	} else {
		// apply adjustment from the given offset...
		offset += cOffset
	}

	// save the new offset
	b.consumers[c] = offset
	// we (may have) modified the buffer, broadcast it
	b.cond.Broadcast()

	return nil
}

// get attempts to get a value, by a consumer, with it's internal offset (which is an ADJUSTMENT to the consumer's
// actual offset) returning false if not yet available, and error if not possible, note this method does not lock
func (b *Buffer) get(c *consumer, offset int) (interface{}, bool, error) {
	// check we haven't got a cancelled context
	if err := b.ctx.Err(); err != nil {
		return nil, false, err
	}

	// retrieve the stored offset for the consumer
	if cOffset, ok := b.consumers[c]; !ok {
		return nil, false, fmt.Errorf("bigbuff.Buffer.get unknown consumer: %p", c)
	} else {
		// apply adjustment from the given offset...
		offset += cOffset
	}

	// the relative offset is the actual index in the buffer
	relative := offset - b.offset

	// guard against past offsets
	if relative < 0 {
		return nil, false, fmt.Errorf("bigbuff.Buffer.get offset %d is %d past", offset, -1*relative)
	}

	// guard against pending offsets (as in, further down b.buffer than is available)
	if relative >= len(b.buffer) {
		return nil, false, nil
	}

	// we had the value available, return it
	return b.buffer[relative], true, nil
}

// getAsync returns a channel that will be sent the next value, for the offset stored against a given consumer, modified
// by an additional offset value, or a synchronous value or synchronous error, depending on the state, where non-nil
// error indicates sync error, a non-nil channel indicates async response, and both chan and error nil indicating sync
// success with value (possibly nil).
// Any error means error, no error and any channel means async, and nil channel and nil error means a value.
// NOTE: if a channel is returned, it will NEVER be closed, to prevent empty reads.
func (b *Buffer) getAsync(ctx context.Context, c *consumer, offset int, cancels ...context.Context) (<-chan struct {
	Value interface{}
	Error error
}, interface{}, error) {
	// read lock for the first (sync) attempt at getting the value
	// the async case returns a channel, so this will be released in that case
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	// initial check - can we get the state?
	if v, ok, err := b.get(c, offset); err != nil {
		// sync err case
		return nil, nil, err
	} else if ok {
		// sync success case
		return nil, v, nil
	}

	// async case (no error and false for the sync get)

	// create the output channel
	out := make(chan struct {
		Value interface{}
		Error error
	}, 1)

	// spawn a sender for it
	go func() {
		// we need to wait for the value in the buffer, so we need to write lock the buffer
		b.mutex.Lock()
		defer b.mutex.Unlock()

		// to break it down, while the input context is open AND the the buffer context is open AND all cancels
		// (contexts provided as the last vararg) are open, block on changes to the buffer, running the sync get as
		// before, until it either fails with error, or it returns true with a value.
		var result struct {
			Value interface{}
			Error error
		}
		if err := WaitCond(
			CombineContext(
				ctx, // the input context is used as the master
				append(
					append(
						make([]context.Context, 0, len(cancels)+1),
						b.ctx,
					),
					cancels...,
				)...,
			),      // combined context is used (all components, base on input though)
			b.cond, // wait on the buffer's cond
			func() bool {
				// initial check - can we get the state?
				if v, ok, err := b.get(c, offset); err != nil {
					// async error case
					result.Error = err
					return true // exits the wait loop
				} else if ok {
					// async success case
					result.Value = v
					return true // exits the wait loop
				}
				return false // still no result or error, continue to wait
			}, // waits until get either errors or succeeds (or context cancel)
		); err != nil && result.Error == nil { // avoid overwriting internal errors with context errors
			// async error case due to context error
			result.Error = err
		}
		out <- result
	}()

	// return the channel so the async case can be resolved in the consumer logic
	// this is important since we need to release the read lock anyway
	return out, nil, nil
}

// ensure both sets up the *Buffer, and acts as a panic-causing guard against nil receivers
// NOTE: this doesn't provide any guarantees on sync, and VERY CAREFULLY uses double checked init
func (b *Buffer) ensure() {
	if b == nil {
		panic(errors.New("bigbuff.Buffer encountered a nil receiver"))
	}
	var changes []func()
	defer func() {
		if len(changes) == 0 {
			return
		}
		b.mutex.Lock()
		defer b.mutex.Unlock()
		for _, change := range changes {
			change()
		}
	}()
	if b.ctx == nil {
		changes = append(changes, func() {
			if b.ctx == nil {
				b.ctx = context.Background()
			}
		})
	}
	if b.cancel == nil {
		changes = append(changes, func() {
			if b.cancel == nil {
				b.ctx, b.cancel = context.WithCancel(b.ctx)
			}
		})
	}
	if b.consumers == nil {
		changes = append(changes, func() {
			if b.consumers == nil {
				b.consumers = make(map[*consumer]int)
			}
		})
	}
	if b.done == nil {
		changes = append(changes, func() {
			if b.done == nil {
				// b.done isn't buffered, it will never be sent any data anyway, only closed
				b.done = make(chan struct{})
			}
		})
	}
	if b.cleaner == nil {
		changes = append(changes, func() {
			if b.cleaner == nil {
				// setup the cleaner defaults - it's done here to allow new(bigbuff.Buffer)
				// note that b.cleaner.Cleaner must never be nil, and b.cleaner.Cooldown must always be >= 0
				b.cleaner = &CleanerConfig{
					Cleaner:  DefaultCleaner,
					Cooldown: DefaultCleanerCooldown,
				}
			}
		})
	}
	if b.cond == nil {
		changes = append(changes, func() {
			if b.cond == nil {
				// b.cond gets initialised with the write mode of the buffer's rw mutex
				b.cond = sync.NewCond(&b.mutex)
				// start the cleanup process - the goroutine that un-buffers (see CleanerConfig)
				go b.cleanup()
			}
		})
	}
}

// consumerOffsets returns a list of relative offsets for all consumers, note it isn't synced.
func (b *Buffer) consumerOffsets() []int {
	if b == nil || b.consumers == nil {
		return nil
	}
	result := make([]int, 0, len(b.consumers))
	for _, offset := range b.consumers {
		// the relative offset is the consumer's offset - the buffer's offset
		result = append(result, offset-b.offset)
	}
	return result
}

// cleanup is run in a new goroutine on init of b.cond, for the life of the buffer, that checks state on change
// it should only ever be run once per buffer, started on the first use
func (b *Buffer) cleanup() {
	// close the buffer on shutdown (e.g. panic in cleaner)
	defer b.Close()

	// we need to perform "waits" where broadcast will be triggered at some point in the future (only one at a time)
	// to achieve this, a cleanup function is implemented which performs the actual work
	var (
		mutex                 = new(sync.Mutex)
		timer     *time.Timer = nil  // a non-nil values indicates we cannot shift yet as we are waiting
		broadcast             = true // set to true if we receive any updates while we are pending
		cleanup               = func(d time.Duration) {
			// lock so we can check timer safely
			mutex.Lock()
			defer mutex.Unlock()

			// if timer is not nil we are currently waiting, we can just exit
			// NOTE: this is ok because it re-broadcasts at the end of the wait, meaning we re-check, which also
			// means that we must lock mutex when we do the broadcasting to avoid a race there (unless we are ok
			// skipping checks, which is probably fine, but whatever)
			if timer != nil {
				// indicate that we want a re-broadcast (since we missed out this time)
				broadcast = true
				return
			}

			// do the actual cleanup logic, note that though it returns a bool indicating if it actually did anything,
			// the current implementation applies the cooldown regardless of if it did anything
			b.cleanupLogic()

			// no wait?
			if d <= 0 {
				// we don't have any need to wait, since we didn't cleanup
				return
			}

			// spin up a new timer (disables further work until it is cleared)
			timer = time.NewTimer(d)
			// clear any existing broadcast flag
			broadcast = false

			// and block on it via a self removing goroutine
			go func() {
				defer timer.Stop() // just in case, ensure the timer gets stopped
				defer func() {
					// lock on the mutex, so that the timer removal and broadcast checking / performing is synced
					mutex.Lock()
					defer mutex.Unlock()

					// re-enable the cleanup cycle
					timer = nil

					// re-broadcast if we received any changes while we were waiting
					if broadcast {
						// the state will be re-checked
						b.cond.Broadcast()
						// clear the broadcast flag (it gets set up above too but whatever)
						broadcast = false
					}
				}()

				// wait for the timer to expire
				<-timer.C
			}()
		}
	)

	// we need a write lock for the cond + we write
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// listen to broadcasts until the context is cancelled (it loops)
	if err := WaitCond(
		b.ctx,
		b.cond,
		func() bool {
			// call the cleanup function (note we are write locked here)
			cleanup(b.cleaner.Cooldown)
			return false
		},
	); err != nil && b.ctx.Err() == nil {
		// shouldn't ever reach here, but just in case, if errored without context cancel, panic
		panic(err)
	}
}

// cleanupLogic calls the cleaner, and applies the shift it returns then returns true, or returns false if there was
// either nothing to shift, or the cleaner callback returned a value <= 0.
func (b *Buffer) cleanupLogic() bool {
	// call the cleaner (possibly a custom callback) to identify how many to shift
	shift := b.cleaner.Cleaner(
		len(b.buffer),
		b.consumerOffsets(),
	)

	// the upper bound for shift is the length of the buffer
	if l := len(b.buffer); shift > l {
		shift = l
	}

	// if we have no shift to apply, we exit with false to indicate that
	if shift <= 0 {
		return false
	}

	// to safely shift from slice buffer we first set all relevant values to nil (pointers, leaky)
	for x := 0; x < shift; x++ {
		b.buffer[x] = nil
	}

	// update the buffer and the offset - this is the actual shift
	b.buffer = b.buffer[shift:]
	b.offset += shift

	// broadcast that we changed the buffer
	b.cond.Broadcast()

	return true
}

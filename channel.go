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
	"reflect"
	"time"
)

// NewChannel constructs a new consumer that implements Consumer, but receives it's data from a channel, which
// uses reflection to support any readable channel, note that a poll date of zero will use the default, and < 0 is
// an error.
func NewChannel(ctx context.Context, pollRate time.Duration, source interface{}) (*Channel, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if pollRate == 0 {
		pollRate = DefaultChannelPollRate
	}

	if pollRate <= 0 {
		return nil, errors.New("bigbuff.NewChannel poll rate must be > 0, or 0 to use the default")
	}

	if source == nil {
		return nil, errors.New("bigbuff.NewChannel nil source")
	}

	value := reflect.ValueOf(source)

	if value.Kind() != reflect.Chan {
		return nil, fmt.Errorf("bigbuff.NewChannel source (%T) must be a channel", source)
	}

	if chanDir := value.Type().ChanDir(); (chanDir & reflect.RecvDir) != reflect.RecvDir {
		return nil, fmt.Errorf("bigbuff.NewChannel source (%T) must have a receivable direction", source)
	}

	c := &Channel{
		valid:  true,
		source: value,
		done:   make(chan struct{}),
		rate:   pollRate,
	}

	c.ctx, c.cancel = context.WithCancel(ctx)

	// ensure c.Close on context cancel (for the done channel, tl;dr it synchronises the done channel in a sane way)
	go c.cleanup()

	return c, nil
}

// Buffer returns any values that were drained from the source but not committed yet, in a new copy of the internal
// buffer, note that if you are trying to ensure no messages get lost in the void, block until Channel.Done before
// calling this.
func (c *Channel) Buffer() []interface{} {
	c.ensure()

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.buffer == nil {
		return nil
	}

	result := make([]interface{}, len(c.buffer))
	copy(result, c.buffer)

	return result
}

// Close closes the consumer NOTE that it doesn't close the source channel.
func (c *Channel) Close() error {
	c.ensure()

	closeErr := errors.New("bigbuff.Channel.Close may be closed at most once")

	c.close.Do(func() {
		closeErr = nil

		// synchronisation is required to avoid racing on Get
		c.mutex.Lock()
		defer c.mutex.Unlock()

		// close the done channel AFTER the context cancel WHILE holding the mutex
		defer close(c.done)

		// avoid any new state changes
		c.cancel()
	})

	return closeErr
}

func (c *Channel) Done() <-chan struct{} {
	c.ensure()

	// no sync required for getting this but just in case...
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.done
}

func (c *Channel) Get(ctx context.Context) (value interface{}, err error) {
	c.ensure()

	// init on first failure, and re-used from then
	var ticker *time.Ticker

	for {
		// we can guard the input context before locking (but it still needs to be in the loop)
		if ctx != nil {
			err = ctx.Err()
			if err != nil {
				return
			}
		}

		// attempt a get, returns true if we exit this iteration
		if func() bool {
			// synchronise - we will break the state otherwise
			c.mutex.Lock()
			defer c.mutex.Unlock()

			// check for context cancels - bails out if so, we must not modify the state further
			err = c.ctx.Err()
			if err != nil {
				return true
			}

			// branch for the case that we have rolled back, and need to read from the buffer instead
			if c.rollback > 0 {
				value = c.buffer[c.pending()] // value read from the next pending value
				c.rollback--                  // increments the next pending value for commit / rollback state
				return true
			}

			// attempt a read from the channel
			v, ok := c.source.TryRecv()

			// ok indicates a value is available and it's not closed
			if ok {
				value = v.Interface()
				c.buffer = append(c.buffer, value) // value appended to the buffer, for commit or rollback
				return true
			}

			// a value wasn't available this time (we cannot handle the closed channel state meaningfully here)
			return false
		}() {
			return
		}

		// we have released the mutex, wait a bit before polling again

		if ticker == nil { // do we need to init the ticker
			ticker = time.NewTicker(c.rate)
			//noinspection GoDeferInLoop
			defer ticker.Stop()
		}

		select {
		case <-ticker.C:
			// tick triggers the next iteration
		case <-c.ctx.Done():
			// as does a context cancel (which will bail out next iteration)
		}
	}
}

// pending is the number of buffered items that are not rolled back
func (c *Channel) pending() int {
	return len(c.buffer) - c.rollback
}

// Commit resets read parts of the buffer, or returns an error, note it will always error after context cancel.
func (c *Channel) Commit() error {
	c.ensure()

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if err := c.ctx.Err(); err != nil {
		return err
	}

	pending := c.pending()

	if pending == 0 {
		return errors.New("bigbuff.Channel.Commit nothing to commit")
	}

	// clear refs to ensure no memory leaks via the buffer slice
	for i := 0; i < pending; i++ {
		c.buffer[i] = nil
	}

	// shift pending items off the front of the buffer
	c.buffer = c.buffer[pending:]

	return nil
}

// Rollback will cause following Get calls to read from the start of the buffer, or it will return an error if
// there is nothing to rollback (there is nothing pending).
func (c *Channel) Rollback() error {
	c.ensure()

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// rollback can happen regardless of context

	pending := c.pending()

	if pending == 0 {
		return errors.New("bigbuff.Channel.Rollback nothing to rollback")
	}

	c.rollback += pending

	return nil
}

func (c *Channel) ensure() {
	if c == nil || !c.valid {
		panic(errors.New("bigbuff.Channel must be instanced via bigbuff.NewChannel"))
	}
}

func (c *Channel) cleanup() {
	defer c.Close()
	<-c.ctx.Done()
}

// Package bigbuff implements a one-many ordered queue producer-consumer pattern + utilities.
package bigbuff

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"
)

const (
	// DefaultCleanerCooldown is how long the cleaner will wait between checks of the Buffer by default.
	DefaultCleanerCooldown = time.Millisecond * 10

	// DefaultChannelPollRate is how frequently each waiting Channel.Get should try to receive from the channel (from
	// the first failure/non-receive).
	DefaultChannelPollRate = time.Millisecond
)

type (
	// Producer models a producer in a producer-consumer pattern, where the resource will be closed at most once.
	Producer interface {
		io.Closer

		// Done should return a channel that will be closed after internal resources have been freed, after a `Close`
		// call, which may not be explicit.  This *may* mean that it blocks on any pending changes, and it *may* also
		// be possible that the consumer will be closed due to external reasons, e.g. connection closing.
		Done() <-chan struct{}

		// Put will send the provided values in-order to the message buffer, or return an error.
		// It MUST NOT block in such a way that it will be possible to cause a deadlock locally.
		Put(ctx context.Context, values ... interface{}) error
	}

	// Consumer models a consumer in a producer-consumer pattern, where the resource will be closed at most once.
	Consumer interface {
		io.Closer

		// Done should return a channel that will be closed after internal resources have been freed, after a `Close`
		// call, which may not be explicit. This *may* mean that it blocks on any pending changes, and it *may* also
		// be possible that the consumer will be closed due to external reasons, e.g. connection closing.
		Done() <-chan struct{}

		// Get will get a message from the message buffer, at the current offset, blocking if none are available, or
		// an error if it fails.
		Get(ctx context.Context) (interface{}, error)

		// Commit will save any offset changes, and will return an error if it fails, or if the offset saved is the
		// latest.
		Commit() error

		// Rollback will undo any offset changes, and will return an error if it fails, or if the offset saved is the
		// latest.
		Rollback() error
	}

	// Buffer is the core implementation, implementing the Producer interface, and providing auxiliary methods for
	// configuration, as well as `NewConsumer`, to instance a new consumer, note that though it is safe to instance
	// via new(bigbuff.Buffer), it must not be copied after first use.
	// It's behavior regarding message retention may be configured via SetCleanerConfig, by default it will un-buffer
	// only messages that have been read by all "active and valid" consumers, given at least one exists, otherwise it
	// will retain messages indefinitely.
	// NOTE: the buffer itself will not be cleared even after close, so the data can still be accessed.
	Buffer struct {
		mutex     sync.RWMutex       // mutex is rw, for the internal state
		cond      *sync.Cond         // cond broadcasts all state changes
		done      chan struct{}      // done will be closed when all resources are freed
		close     sync.Once          // close is used to ensure that only one close call works
		ctx       context.Context    // ctx is used to free dependant resources
		cancel    context.CancelFunc // cancel closes context
		consumers map[*consumer]int  // consumers maps all open consumers to their COMMITTED offsets (self-removing)
		buffer    []interface{}      // buffer is the internal buffer
		offset    int                // offset increments with each message removed from the buffer
		cleaner   *CleanerConfig     // cleaner is used to configure the logic to maintain the buffer's size
	}

	// Cleaner is a callback used to manage the size of a bigbuff.Buffer instance, it will be called when relevant
	// to do so, with the size of the buffer, and the consumer offsets (relative to the buffer), and should return
	// the number of elements from the buffer that should be attempted to be shifted from the buffer.
	Cleaner func(size int, offsets []int) int

	// CleanerConfig is a configuration for a bigbuff.Buffer, that defines how the size is managed
	CleanerConfig struct {
		// Cleaner is used to determine if items are removed from the buffer
		Cleaner Cleaner

		// Cooldown is the minimum time between cleanup cycles
		Cooldown time.Duration
	}

	// FixedBufferCleanerNotification is the context provided to the optional callback provided to the
	// FixedBufferCleaner function.
	FixedBufferCleanerNotification struct {
		Max     int   // Max size before forced cleanup is triggered.
		Target  int   // Target size when force cleanup is triggered.
		Size    int   // Size when cleanup was triggered.
		Offsets []int // Offsets when cleanup was triggered.
		Trim    int   // Trim number returned.
	}

	// Channel implements Consumer based on data from a channel, note that because it uses reflection and polling
	// internally, it is actually safe to close the input channel without invalid zero value reads (though the
	// Channel itself still needs to be closed, and any Get calls will still be blocked until then).
	Channel struct {
		mutex    sync.Mutex         // mutex synchronises the (rollback) buffer, blocked count, and closes
		valid    bool               // valid is set when correctly instanced via NewChannel
		source   reflect.Value      // source is the input channel
		buffer   []interface{}      // buffer is anything received not committed yet (Channel.Buffer() returns it)
		ctx      context.Context    // ctx manages "close state" (not actual channel close)
		cancel   context.CancelFunc // cancel is used to close the Channel
		close    sync.Once          // close once :)
		done     chan struct{}      // done is closed once no more values can be read
		rate     time.Duration      // rate is the channel poll rate on failed receives
		rollback int                // rollback is the number of items in the buffer that need to be Get again
	}

	// consumer is the internal implementation of a consumer that is tied to an in-memory buffer.
	consumer struct {
		mutex    sync.Mutex         // mutex provides synchronisation for the consumer offset value
		cond     *sync.Cond         // cond broadcasts all state changes
		done     chan struct{}      // done will be closed when all resources are freed
		close    sync.Once          // close is used to ensure that only one close call works
		ctx      context.Context    // ctx is used to avoid go routine leak (consumer closed but not the buffer)
		cancel   context.CancelFunc // cancel is used to avoid go routine leak (consumer closed but not the buffer)
		producer producer           // producer is the instance that this consumer is for
		offset   int                // offset is the current read offset (a diff on the offset stored in the buffer)
	}

	// producer models the relevant unexported functions of the buffer used by the consumer, for testing
	producer interface {
		delete(c *consumer)
		getAsync(ctx context.Context, c *consumer, offset int, cancels ... context.Context) (<-chan struct {
			Value interface{}
			Error error
		}, interface{}, error)
		commit(c *consumer, offset int) error
	}

	// Exclusive provides synchronous de-bouncing of operations that may also return a result or error, with
	// consistent or controlled input via provided closures also supported, and the use of any comparable keys to
	// match on, it provides a guarantee that the actual call that returns a given value will be started AFTER the
	// Call method, so keep that in mind when implementing something using it. You may also use the CallAfter
	// method to delay execution after initialising the key, e.g. to allow the first of many costly operations on
	// a given key a grace period to be grouped with the remaining keys.
	Exclusive struct {
		mutex sync.Mutex
		work  map[interface{}]*exclusiveItem
	}

	// ExclusiveOutcome is the return value from an async bigbuff.Exclusive call
	ExclusiveOutcome struct {
		Result interface{}
		Error  error
	}

	exclusiveItem struct {
		mutex    *sync.Mutex
		cond     *sync.Cond
		work     func() (interface{}, error)
		running  bool
		complete bool
		count    int
		result   interface{}
		err      error
	}
)

// DefaultCleaner is the Buffer's default cleaner, if there is at least one "active" consumer it returns the lowest
// offset, defaulting to 0, effectively removing values from the buffer that all consumers have read, note the return
// value is limited to >= 0  and <= size, active consumers are defined as those registered with offsets >= 0.
func DefaultCleaner(size int, offsets []int) int {
	lowest := size
	active := false
	for _, offset := range offsets {
		// 0 is always going to be the lowest, we can bail out
		if offset == 0 {
			return 0
		}
		// we need to ignore negative values
		if offset < 0 {
			continue
		}
		// we had at least one active consumer
		active = true
		// update the lowest if the current offset is lower
		if offset < lowest {
			lowest = offset
		}
	}
	// if we didn't have any active consumers, no removals are allowed
	if !active {
		return 0
	}
	// return the lowest (only 0 here if the size was 0, otherwise > 0)
	return lowest
}

// FixedBufferCleaner builds a cleaner that will give a buffer a fixed threshold size, which will trigger
// forced reduction back to a fixed target size, note that if callback is supplied it will be called with the details
// of the cleanup, in the event that it forces cleanup past the default.
// This has the effect of causing any consumers that were running behind the target size (in terms of their read
// position in the buffer) to fail on any further Get calls.
func FixedBufferCleaner(
	max int,                                                    // max size before forced cleanup is triggered
	target int,                                                 // target size when forced cleanup is triggered
	callback func(notification FixedBufferCleanerNotification), // (optional) callback hooks any forced cleanups
) Cleaner {
	return func(size int, offsets []int) int {
		if size > max {
			trim := size - target
			if callback != nil {
				callback(FixedBufferCleanerNotification{
					Max:     max,
					Target:  target,
					Size:    size,
					Offsets: offsets,
					Trim:    trim,
				})
			}
			return trim
		}
		return DefaultCleaner(size, offsets)
	}
}

// Range iterates over the consumer, encapsulating automatic commits and rollbacks, including rollbacks caused
// by panics, note that the index will be the index in THIS range, starting at 0, and incrementing by one with each
// call to fn.
// NOTE: the ctx value will be passed into the consumer.Get as-is.
func Range(ctx context.Context, consumer Consumer, fn func(index int, value interface{}) bool) error {
	if consumer == nil {
		return errors.New("bigbuff.Range requires non-nil consumer")
	}

	if fn == nil {
		return errors.New("bigbuff.Range requires non-nil fn")
	}

	var (
		fatalErr error = nil
		running  bool  = true
		// rollback handles any rollback + any error including any required append logic to get a full fatal error
		rollback = func() {
			err := consumer.Rollback()
			if err == nil {
				return
			}
			err = fmt.Errorf("bigbuff.Range rollback error: %s", err.Error())
			if fatalErr == nil {
				fatalErr = err
				return
			}
			fatalErr = fmt.Errorf("%s\n  - %s", fatalErr.Error(), err.Error())
		}
	)

	// while we are running (haven't returned false from fn) and there are no fatal errors
	for index := 0; running && fatalErr == nil; index++ {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("bigbuff.Range context error: %s", err.Error())
			}
		}

		// closure so we can recover via defer without memory leaks
		func() {
			// catch / throw pattern for rollbacks
			defer func() {
				r := recover()
				if r == nil {
					return
				}
				rollback()
				panic(r)
			}()

			if value, err := consumer.Get(ctx); err != nil { // perform a get, setting fatal error if it fails
				fatalErr = fmt.Errorf("bigbuff.Range get error: %s", err.Error())
			} else if !fn(index, value) { // if we got a value, call fn with the (index, value), handles stop
				running = false
			}

			// EITHER commit (no error) OR rollback (error)
			if fatalErr == nil {
				// commit failures also set fatal error
				if err := consumer.Commit(); err != nil {
					fatalErr = fmt.Errorf("bigbuff.Range commit error: %s", err.Error())
				}
			} else {
				// we had a fatal error in this iteration, rollback before exit
				rollback()
			}
		}()
	}

	// may be nil, or may contain any and all fatal errors that occurred in this function
	return fatalErr
}

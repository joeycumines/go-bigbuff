package bigbuff

import (
	"context"
	"errors"
	"fmt"
)

// Close will close an open consumer, potentially blocking until the internal offset has been reset (changes have
// been committed or rolled back), note that Close will only return non-nil once, which may occur automatically due
// to context cancel.
func (c *consumer) Close() (err error) {
	err = errors.New("bigbuff.consumer.Close may only be called once")

	c.close.Do(func() {
		err = nil

		// we need to wait for any pending offsets, so lock
		c.mutex.Lock()
		defer c.mutex.Unlock()

		// all resources should be freed after this call - we will close the done channel
		defer close(c.done)

		// as a final step before marking as done, we need to self-remove from the buffer
		defer c.producer.delete(c)

		// cancel the context, so that get functions etc know
		c.cancel()

		// block until the offset is 0 (so we don't have uncommitted changes)
		for c.offset != 0 {
			c.cond.Wait()
		}
	})

	return
}

// Done returns the internal done channel, it locks the mutex to make things sync a bit nicer.
func (c *consumer) Done() <-chan struct{} {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.done
}

func (c *consumer) Get(ctx context.Context) (interface{}, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("bigbuff.consumer.Get input context error: %s", err.Error())
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := c.ctx.Err(); err != nil {
		return nil, fmt.Errorf("bigbuff.consumer.Get internal context error: %s", err.Error())
	}

	out, v, err := c.producer.getAsync(ctx, c, c.offset, c.ctx)

	if err != nil {
		return nil, fmt.Errorf("bigbuff.consumer.Get sync get error: %s", err.Error())
	}

	if out == nil {
		// nil chan + nil err indicates sync success
		c.offset++
		c.cond.Broadcast()

		return v, nil
	}

	// it was async
	result := <-out

	if result.Error != nil {
		return nil, fmt.Errorf("bigbuff.consumer.Get async get error: %s", result.Error.Error())
	}

	c.offset++
	c.cond.Broadcast()

	return result.Value, nil
}

func (c *consumer) Commit() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.offset == 0 {
		return errors.New("bigbuff.consumer.Commit nothing to commit")
	}

	if err := c.producer.commit(c, c.offset); err != nil {
		return fmt.Errorf("bigbuff.consumer.Commit commit error: %s", err.Error())
	}

	c.offset = 0
	c.cond.Broadcast()

	return nil
}

func (c *consumer) Rollback() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.offset == 0 {
		return errors.New("bigbuff.consumer.Rollback nothing to rollback")
	}

	c.offset = 0
	c.cond.Broadcast()

	return nil
}

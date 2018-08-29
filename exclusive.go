package bigbuff

import (
	"errors"
	"sync"
	"time"
)

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
	if e == nil || value == nil {
		panic(errors.New("bigbuff.Exclusive receiver and value must be non-nil"))
	}

	var (
		ts   = time.Now()
		item *exclusiveItem
	)

	// find or create item
	for {
		// init map and obtain item
		e.mutex.Lock()
		if e.work == nil {
			e.work = make(map[interface{}]*exclusiveItem)
		}
		if _, ok := e.work[key]; !ok {
			e.work[key] = new(exclusiveItem)
			e.work[key].mutex = new(sync.Mutex)
			e.work[key].cond = sync.NewCond(e.work[key].mutex)
		}
		item = e.work[key]
		e.mutex.Unlock()

		// lock item then the root to check if item is still valid
		item.mutex.Lock()
		var valid bool
		e.mutex.Lock()
		if v, _ := e.work[key]; v == item {
			valid = true
		}
		e.mutex.Unlock()
		if valid {
			// keep the item lock to prevent it from being no longer valid
			// (we remove from the map after obtaining item > root, in the same way)
			break
		}
		item.mutex.Unlock()
	}

	// always update the work and increment count
	item.work = value
	item.count++

	// wait until not running, which has two cases
	// 1) newly initialised item or item initialised while running
	// 2) item has been completed
	for item.running {
		item.cond.Wait()
	}

	if item.complete {
		// case 2)
		defer item.mutex.Unlock()
		return item.output, item.err
	}

	// case 1)

	// the item is now running
	item.running = true

	// before we remove item from the work map, handle any specified wait, unlocking while we are waiting
	// so that other calls may register themselves on the item
	if wait > 0 {
		// adjust the sleep by how long we have already waited
		wait -= time.Now().Sub(ts)
		item.mutex.Unlock()
		time.Sleep(wait)
		item.mutex.Lock()
	}

	// replace the item in the work map with a new one sharing the same mutex and cond and also running
	// (we still use our current item, but we only want calls started BEFORE this one to share the same result)
	e.mutex.Lock()
	e.work[key] = &exclusiveItem{
		mutex:   item.mutex,
		cond:    item.cond,
		running: true,
	}
	e.mutex.Unlock()

	// release the mutex while we do the work
	item.mutex.Unlock()

	var (
		output interface{}
		err    = errors.New("unknown error")
	)

	defer func() {
		item.mutex.Lock()
		defer item.mutex.Unlock()

		e.mutex.Lock()
		defer e.mutex.Unlock()

		defer item.cond.Broadcast()

		item.output = output
		item.err = err
		item.complete = true
		item.running = false

		e.work[key].running = false

		if e.work[key].count == 0 {
			delete(e.work, key)
		}
	}()

	output, err = item.work()

	return output, err
}

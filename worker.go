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
	"errors"
	"sync"
)

// Worker implements a background worker pattern, providing synchronisation around running at most a single
// worker, for n number of callers (of it's Do method).
//
// Note that although the sync.WaitGroup is used internally, sync.Once is still preferable for simpler cases.
type Worker struct {
	mu   sync.Mutex
	wg   *sync.WaitGroup
	stop chan struct{}
	done chan struct{}
}

// Do will call fn in a new goroutine, if the receiver is not already running, and will always return a done func
// which must be called, to indicate when the worker is no longer in use. Once no callers are using a worker, that
// worker will be stopped. Stopping involves closing the stop channel, causing further calls to Do to block, until
// the worker finishes. A panic will occur if either the receiver or fn are nil.
func (x *Worker) Do(fn func(stop <-chan struct{})) (done func()) {
	if x == nil || fn == nil {
		panic(errors.New(`bigbuff.Worker invalid input`))
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.stop == nil && x.done == nil {
		x.stop, x.done = make(chan struct{}), make(chan struct{})
		go x.wait()
		go x.do(fn)
	}
	if x.wg == nil {
		x.wg = new(sync.WaitGroup)
	}
	x.wg.Add(1)
	return x.wg.Done
}
func (x *Worker) wait() {
	for {
		x.mu.Lock()
		wg := x.wg
		if wg == nil {
			break
		}
		x.wg = nil
		x.mu.Unlock()
		wg.Wait()
	}
	close(x.stop)
	<-x.done
	x.stop, x.done = nil, nil
	x.mu.Unlock()
}
func (x *Worker) do(fn func(stop <-chan struct{})) {
	fn(x.stop)
	close(x.done)
}

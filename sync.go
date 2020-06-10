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
	"sync"
)

// WaitCond performs a conditional wait against a *sync.Cond, waiting until fn returns true, with a inbuilt escape
// hatch for context cancel.
// Note that the relevant locker must be locked before this is called. It should also be noted that cond.L.Lock will
// before a context triggered broadcast, in order to avoid a race condition (i.e. if context is cancelled while fn
// is being evaluated).
func WaitCond(ctx context.Context, cond *sync.Cond, fn func() bool) error {
	if cond == nil {
		return errors.New("bigbuff.WaitCond requires a non-nil cond")
	}
	if cond.L == nil {
		return errors.New("bigbuff.WaitCond requires a cond with a non-nil locker")
	}
	if fn == nil {
		return errors.New("bigbuff.WaitCond requires a non-nil fn")
	}
	var cancel context.CancelFunc
	for {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
			if cancel == nil {
				ctx, cancel = context.WithCancel(ctx)
				//noinspection GoDeferInLoop
				defer cancel()
				go func() {
					<-ctx.Done()
					locked := false
					if l := cond.L; l != nil {
						locked = true
						l.Lock()
						defer l.Unlock()
					}
					cond.Broadcast()
					if !locked {
						panic(errors.New("bigbuff.WaitCond unable to lock while triggering a broadcast due to context cancel"))
					}
				}()
			}
		}
		if fn() {
			return nil
		}
		cond.Wait()
	}
}

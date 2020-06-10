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
	"fmt"
	"reflect"
)

// Subscribe is equivalent of SubscribeContext(nil, key, target)
func (n *Notifier) Subscribe(key interface{}, target interface{}) {
	n.SubscribeContext(nil, key, target)
}

// SubscribeContext registers a given target channel as a subscriber for a given key, which will block any attempts
// to publish to the key unless it is received from appropriately, or until context cancel (if a non-nil context was
// provided), be sure to unsubscribe exactly once to free references to ctx and target. A panic will occur if target is
// not a channel to which the notifier can send, or if there already exists a subscription for the given key and target
// combination. The key may be any comparable value.
func (n *Notifier) SubscribeContext(ctx context.Context, key interface{}, target interface{}) {
	var (
		value    = valueOfNotifierTarget(target)
		valuePtr = value.Pointer()
	)

	n.mutex.Lock()
	defer n.mutex.Unlock()

	subscribers := n.subscribers
	if subscribers == nil {
		subscribers = make(map[interface{}]map[uintptr]notifierSubscriber)
	}
	keySubscribers, _ := subscribers[key]
	if keySubscribers == nil {
		keySubscribers = make(map[uintptr]notifierSubscriber)
	}

	subscriber, ok := keySubscribers[valuePtr]
	if ok {
		panic(fmt.Errorf(`bigbuff.Notifier subscription already exists: key=%v target=%v`, key, valuePtr))
	}
	subscriber.ctx = ctx
	subscriber.target = value

	if ctx != nil && ctx.Err() != nil {
		return
	}

	keySubscribers[valuePtr] = subscriber
	subscribers[key] = keySubscribers
	n.subscribers = subscribers
}

// SubscribeCancel wraps SubscribeContext and Unsubscribe as well as the initialisation of a sub context, for defer
// statements using the result as a one-liner, and is the most fool-proof way to implement a subscriber, at the cost
// of less direct management of resources (including some which are potentially unnecessary, as it uses a sub-context
// and the returned cancel obeys the contract of context.CancelFunc and does not perform Unsubscribe inline)
func (n *Notifier) SubscribeCancel(ctx context.Context, key interface{}, target interface{}) context.CancelFunc {
	if ctx == nil {
		ctx = context.Background()
	}
	var (
		success bool
		cancel  context.CancelFunc
	)
	ctx, cancel = context.WithCancel(ctx)
	defer func() {
		if !success {
			cancel()
		}
	}()
	n.SubscribeContext(ctx, key, target)
	go func() {
		<-ctx.Done()
		n.Unsubscribe(key, target)
	}()
	success = true
	return cancel
}

// Unsubscribe deregisters a given key and target from the notifier, an action that may be performed exactly once
// after each subscription (for the combination of key and target), preventing further messages from being published
// to the target, and allowing freeing of associated resources WARNING subscribe context should always be canceled
// before calling this, or it may deadlock (especially under load)
func (n *Notifier) Unsubscribe(key interface{}, target interface{}) {
	var (
		value    = valueOfNotifierTarget(target)
		valuePtr = value.Pointer()
	)

	n.mutex.Lock()
	defer n.mutex.Unlock()

	if subscribers := n.subscribers; subscribers != nil {
		if keySubscribers, _ := subscribers[key]; keySubscribers != nil {
			if _, ok := keySubscribers[valuePtr]; ok {
				delete(keySubscribers, valuePtr)
				if len(keySubscribers) == 0 {
					delete(subscribers, key)
					if len(subscribers) == 0 {
						n.subscribers = nil
					}
				}
				return
			}
		}
	}

	panic(fmt.Errorf(`bigbuff.Notifier subscription not found: key=%v target=%v`, key, valuePtr))
}

// Publish is equivalent of PublishContext(nil, key, value)
func (n *Notifier) Publish(key interface{}, value interface{}) {
	n.PublishContext(nil, key, value)
}

// PublishContext will send value to the targets of all active subscribers for a given key for which value is
// assignable, blocking until ctx is canceled (if non-nil), or each relevant subscriber is either sent value or
// cancels it's context
func (n *Notifier) PublishContext(ctx context.Context, key interface{}, value interface{}) {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	if ctx != nil && ctx.Err() != nil {
		return
	}

	if n.subscribers == nil {
		return
	}
	keySubscribers, _ := n.subscribers[key]
	if keySubscribers == nil {
		return
	}

	var (
		valueRef     = reflect.ValueOf(value)
		exitCases    []reflect.SelectCase
		failureCases []reflect.SelectCase
		failureRefs  []int
		successCases []reflect.SelectCase
	)
	if ctx != nil {
		exitCases = append(exitCases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())})
	}
	for _, keySubscriber := range keySubscribers {
		if keySubscriber.ctx != nil && keySubscriber.ctx.Err() != nil {
			continue
		}
		if !valueRef.Type().AssignableTo(keySubscriber.target.Type().Elem()) {
			continue
		}
		if keySubscriber.ctx != nil {
			failureCases = append(failureCases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(keySubscriber.ctx.Done())})
			failureRefs = append(failureRefs, len(successCases))
		}
		successCases = append(successCases, reflect.SelectCase{Dir: reflect.SelectSend, Chan: keySubscriber.target, Send: valueRef})
	}

	for len(successCases) != 0 {
		var (
			exitIndex, _, _ = reflect.Select(append(append(append(make([]reflect.SelectCase, 0, len(exitCases)+len(failureCases)+len(successCases)), exitCases...), failureCases...), successCases...))
			failureIndex    = exitIndex - len(exitCases)
			successIndex    = failureIndex - len(failureCases)
		)

		switch {
		case exitIndex < len(exitCases):
			return

		case failureIndex < len(failureCases):
			//noinspection GoNilness
			successIndex = failureRefs[failureIndex]

		default:
			failureIndex = -1
			for i, failureRef := range failureRefs {
				if failureRef == successIndex {
					failureIndex = i
					break
				}
			}
		}

		for i := len(failureRefs) - 1; i >= 0; i-- {
			if failureRefs[i] <= successIndex {
				break
			}
			failureRefs[i]--
		}

		copy(successCases[successIndex:], successCases[successIndex+1:])
		successCases[len(successCases)-1] = reflect.SelectCase{}
		successCases = successCases[:len(successCases)-1]

		if failureIndex < 0 {
			continue
		}

		copy(failureCases[failureIndex:], failureCases[failureIndex+1:])
		//noinspection GoNilness
		failureCases[len(failureCases)-1] = reflect.SelectCase{}
		failureCases = failureCases[:len(failureCases)-1]

		copy(failureRefs[failureIndex:], failureRefs[failureIndex+1:])
		failureRefs = failureRefs[:len(failureRefs)-1]
	}
}

func valueOfNotifierTarget(target interface{}) reflect.Value {
	value := reflect.ValueOf(target)
	if kind := value.Kind(); kind != reflect.Chan {
		panic(fmt.Errorf(`bigbuff.Notifier invalid target kind: %s`, kind.String()))
	}
	if chanDir := value.Type().ChanDir(); (chanDir & reflect.SendDir) != reflect.SendDir {
		panic(fmt.Errorf(`bigbuff.Notifier invalid target channel direction: %s`, chanDir.String()))
	}
	return value
}

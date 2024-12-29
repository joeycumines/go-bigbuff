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
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConflatedContext_panicOnNoInput(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic, got none")
		}
	}()
	ConflatedContext()
}

func TestConflatedContext_allCanceledInitially(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	// Create contexts that are all canceled.
	canceledCtx1, cancel1 := context.WithCancel(context.Background())
	cancel1()
	canceledCtx2, cancel2 := context.WithCancel(context.Background())
	cancel2()

	ctx, _ := ConflatedContext(canceledCtx1, canceledCtx2)

	select {
	case <-ctx.Done():
		// Expected: should already be canceled
	default:
		t.Errorf("ConflatedContext should have been canceled immediately, but was not")
	}

	if err := ctx.Err(); err != context.Canceled {
		t.Errorf("expected context to be canceled, got %v", err)
	}
}

func TestConflatedContext_oneActiveOneCanceled(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	activeCtx, cancelActive := context.WithCancel(context.Background())
	canceledCtx, cancelCanceled := context.WithCancel(context.Background())
	cancelCanceled() // canceled from the start

	ctx, conflatedCancel := ConflatedContext(canceledCtx, activeCtx)
	defer conflatedCancel()

	select {
	case <-ctx.Done():
		t.Errorf("ctx is canceled prematurely while activeCtx is still active")
	default:
		// Good: ctx should still be alive
	}

	// Now cancel the active one
	cancelActive()

	// Once the active one is canceled, the conflated ctx should follow
	select {
	case <-ctx.Done():
		// Expected
	case <-time.After(50 * time.Millisecond):
		t.Errorf("ctx did not cancel after the last active context was canceled")
	}
}

func TestConflatedContext_remainsActiveIfOneStaysActive(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	ctx3, cancel3 := context.WithCancel(context.Background())

	conCtx, conCancel := ConflatedContext(ctx1, ctx2, ctx3)
	defer conCancel()

	// Cancel ctx1
	cancel1()
	select {
	case <-conCtx.Done():
		t.Errorf("conCtx should remain active because ctx3 and ctx2 are still active")
	default:
		// Good
	}

	// Cancel ctx3
	cancel3()
	select {
	case <-conCtx.Done():
		t.Errorf("conCtx should remain active because ctx2 is still active")
	default:
		// Good
	}

	// Finally, cancel ctx2
	cancel2()
	select {
	case <-conCtx.Done():
		// Expected now, since all have been canceled
	case <-time.After(50 * time.Millisecond):
		t.Errorf("conCtx did not cancel after all input contexts were canceled")
	}
}

func TestConflatedContext_usesFirstContextValues(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	type key string
	const testKey key = "testKey"

	// The first context has a value.
	baseCtx := context.WithValue(context.Background(), testKey, "foo")

	// Additional contexts:
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	// Create a conflated context.
	conCtx, conCancel := ConflatedContext(baseCtx, ctx2)
	defer conCancel()

	// Should be able to retrieve the value from baseCtx
	if val := conCtx.Value(testKey); val != "foo" {
		t.Errorf("expected conflated context to inherit value 'foo'; got %v", val)
	}
}

func TestConflatedContext_explicitCancel(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	conCtx, conCancel := ConflatedContext(ctx1, ctx2)
	defer conCancel()

	// Explicitly cancel the conflated context
	conCancel()

	// Even though neither ctx1 nor ctx2 is canceled, conCtx should now be done
	select {
	case <-conCtx.Done():
		// Expected
	case <-time.After(50 * time.Millisecond):
		t.Errorf("conCtx not canceled after an explicit cancel call")
	}
}

func TestConflatedContext_partialTimeoutCancel(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	ctx1, cancel1 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel1()

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	conCtx, conCancel := ConflatedContext(ctx1, ctx2)
	defer conCancel()

	// Wait until ctx1 times out
	<-time.After(60 * time.Millisecond)

	// At this point, ctx1 is canceled due to timeout, but ctx2 remains active
	if err := conCtx.Err(); err != nil {
		t.Errorf("conCtx should not be canceled yet. Got error: %v", err)
	}

	// Now cancel ctx2
	cancel2()

	select {
	case <-conCtx.Done():
		// Expected since all contexts are done
	case <-time.After(50 * time.Millisecond):
		t.Errorf("conCtx did not cancel after all underlying contexts canceled")
	}
}

func TestChainAfterFunc_callsFuncOnOtherCancel(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	ctx, cancel := context.WithCancel(context.Background())
	other, otherCancel := context.WithCancel(context.Background())
	defer cancel()

	called := make(chan struct{})
	f := func() { close(called) }

	ChainAfterFunc(ctx, other, f)
	otherCancel()

	select {
	case <-time.After(time.Second * 3):
		t.Error("expected f to be called on other cancel")
	case <-called:
	}
}

func TestChainAfterFunc_callsFuncOnCtxCancel(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	ctx, cancel := context.WithCancel(context.Background())
	other, otherCancel := context.WithCancel(context.Background())
	defer otherCancel()

	called := make(chan struct{})
	f := func() { close(called) }

	ChainAfterFunc(ctx, other, f)
	cancel()

	select {
	case <-time.After(time.Second * 3):
		t.Error("expected f to be called on ctx cancel")
	case <-called:
	}
}

func TestChainAfterFunc_doesNotCallFuncIfAlreadyCalled(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	ctx, cancel := context.WithCancel(context.Background())
	other, otherCancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		callCount int32
		once      sync.Once
		done      = make(chan struct{})
	)
	f := func() {
		atomic.AddInt32(&callCount, 1)
		once.Do(func() {
			close(done)
		})
	}

	ChainAfterFunc(ctx, other, f)
	otherCancel()
	cancel()

	time.Sleep(time.Millisecond * 50)

	select {
	case <-time.After(time.Second * 3):
		t.Error("expected f to be called on either ctx cancel")
	case <-done:
	}

	if v := atomic.LoadInt32(&callCount); v != 1 {
		t.Errorf("expected f to be called once, but was called %d times", v)
	}
}

func TestChainAfterFunc_otherAlreadyCanceled(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	other, otherCancel := context.WithCancel(context.Background())
	defer otherCancel()

	var (
		callCount int32
		once      sync.Once
		done      = make(chan struct{})
	)
	f := func() {
		atomic.AddInt32(&callCount, 1)
		once.Do(func() {
			close(done)
		})
	}

	otherCancel()

	ChainAfterFunc(ctx, other, f)

	select {
	case <-time.After(time.Second * 3):
		t.Error("expected f to be called on either ctx cancel")
	case <-done:
	}

	cancel()

	time.Sleep(time.Millisecond * 30)

	if v := atomic.LoadInt32(&callCount); v != 1 {
		t.Errorf("expected f to be called once, but was called %d times", v)
	}
}

func TestCombineContext_each(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	initialGoroutineCount := runtime.NumGoroutine()

	for count := 1; count <= 100; count++ {
		for index := 0; index < count; index++ {
			var (
				others      = make([]context.Context, count)
				indexCancel context.CancelFunc
			)
			for i := 0; i < count; i++ {
				ctx, cancel := context.WithCancel(context.Background())
				_ = cancel // silence go vet
				others[i] = ctx
				if i == index {
					indexCancel = cancel
				}
			}
			ctx := others[0]
			if len(others) == 1 {
				others = nil
			} else {
				others = others[1:]
			}
			ctx = CombineContext(ctx, others...)
			if ctx.Err() != nil {
				t.Error("should not have cancelled yet")
			}
			indexCancel()
			//fmt.Println("before wait... ", runtime.NumGoroutine())
			<-ctx.Done()
			//fmt.Println("after wait... ", runtime.NumGoroutine())
		}
	}

	time.Sleep(time.Millisecond * 100)

	finalGoroutineCount := runtime.NumGoroutine()

	//fmt.Println("final count... ", runtime.NumGoroutine())

	if finalGoroutineCount > initialGoroutineCount {
		t.Fatalf("spawned too many goroutines... leaky? initial=%d;final=%d", initialGoroutineCount, finalGoroutineCount)
	}
}

func TestCombineContext_nils(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	initialGoroutineCount := runtime.NumGoroutine()

	for count := 2; count <= 20; count++ {
		for index := 0; index < count; index++ {
			var (
				others      = make([]context.Context, count)
				indexCancel context.CancelFunc
			)
			for i := 0; i < count; i++ {
				ctx, cancel := context.WithCancel(context.Background())
				_ = cancel // silence go vet
				others[i] = ctx
				if i == index {
					indexCancel = cancel
				}
			}
			others[(index+1)%count] = nil
			ctx := others[0]
			if len(others) == 1 {
				others = nil
			} else {
				others = others[1:]
			}
			ctx = CombineContext(ctx, others...)
			if ctx.Err() != nil {
				t.Error("should not have cancelled yet")
			}
			indexCancel()
			//fmt.Println("before wait... ", runtime.NumGoroutine())
			<-ctx.Done()
			//fmt.Println("after wait... ", runtime.NumGoroutine())
		}
	}

	time.Sleep(time.Millisecond * 100)

	finalGoroutineCount := runtime.NumGoroutine()

	//fmt.Println("final count... ", runtime.NumGoroutine())

	if finalGoroutineCount > initialGoroutineCount {
		t.Fatalf("spawned too many goroutines... leaky? initial=%d;final=%d", initialGoroutineCount, finalGoroutineCount)
	}
}

func TestCombineContext_pairs(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	type TestCase struct {
		Name      string
		Primary   context.Context
		Secondary context.Context
		Test      func(combined context.Context)
	}

	testCases := make([]TestCase, 0)

	func() {
		var (
			name       = `both_nil`
			pCtx, sCtx context.Context
		)
		testCases = append(
			testCases,
			TestCase{
				Name:      name,
				Primary:   pCtx,
				Secondary: sCtx,
				Test: func(combined context.Context) {
					if combined == nil {
						panic("nil return value")
					}
					if combined.Err() != nil {
						panic("expected not canceled")
					}
					if combined != context.Background() {
						panic("expected context.Background")
					}
					if combined.Err() != nil {
						panic("expected canceled")
					}
				},
			},
		)
	}()

	func() {
		var (
			name       = `secondary_nil`
			pCtx, sCtx context.Context
		)

		pCtx = context.WithValue(context.Background(), "twenty_two", 22)

		testCases = append(
			testCases,
			TestCase{
				Name:      name,
				Primary:   pCtx,
				Secondary: sCtx,
				Test: func(combined context.Context) {
					if combined != pCtx {
						panic("nil secondary should use primary")
					}
					if combined.Value("twenty_two") != 22 {
						panic("expected value")
					}
					if combined.Err() != nil {
						panic("expected canceled")
					}
				},
			},
		)
	}()

	func() {
		var (
			name       = `primary_nil`
			pCtx, sCtx context.Context
		)

		sCtx = context.WithValue(context.Background(), "twenty_two", 22)

		testCases = append(
			testCases,
			TestCase{
				Name:      name,
				Primary:   pCtx,
				Secondary: sCtx,
				Test: func(combined context.Context) {
					if combined == nil || combined == sCtx {
						panic("secondary should not be used")
					}
					if combined.Value("twenty_two") != nil {
						panic("expected nil v")
					}
					if combined.Err() != nil {
						panic("expected canceled")
					}
				},
			},
		)
	}()

	func() {
		var (
			name       = `primary_cancelled`
			pCtx, sCtx context.Context
			pCancel    context.CancelFunc
		)

		pCtx, pCancel = context.WithCancel(context.Background())
		pCancel()
		sCtx = context.Background()
		pCtx = context.WithValue(pCtx, "primary", 1)
		sCtx = context.WithValue(sCtx, "secondary", 2)

		testCases = append(
			testCases,
			TestCase{
				Name:      name,
				Primary:   pCtx,
				Secondary: sCtx,
				Test: func(combined context.Context) {
					if combined == nil || combined != pCtx {
						panic("unexpected combined")
					}
					if combined.Value("primary") != 1 {
						panic("primary value not set")
					}
					if combined.Value("secondary") != nil {
						panic("secondary value set")
					}
					if combined.Err() == nil {
						panic("expected canceled")
					}
				},
			},
		)
	}()

	func() {
		var (
			name       = `secondary_cancelled`
			pCtx, sCtx context.Context
			sCancel    context.CancelFunc
		)

		pCtx = context.Background()
		sCtx, sCancel = context.WithCancel(context.Background())
		sCancel()
		pCtx = context.WithValue(pCtx, "primary", 1)
		sCtx = context.WithValue(sCtx, "secondary", 2)

		testCases = append(
			testCases,
			TestCase{
				Name:      name,
				Primary:   pCtx,
				Secondary: sCtx,
				Test: func(combined context.Context) {
					if combined == nil || combined == pCtx || combined == sCtx {
						panic("unexpected combined")
					}
					if combined.Value("primary") != 1 {
						panic("primary value not set")
					}
					if combined.Value("secondary") != nil {
						panic("secondary value set")
					}
					if combined.Err() == nil {
						panic("expected canceled")
					}
				},
			},
		)
	}()

	func() {
		var (
			name       = `primary_cancel`
			pCtx, sCtx context.Context
			pCancel    context.CancelFunc
		)

		pCtx, pCancel = context.WithCancel(context.Background())
		sCtx = context.Background()
		pCtx = context.WithValue(pCtx, "primary", 1)
		sCtx = context.WithValue(sCtx, "secondary", 2)

		testCases = append(
			testCases,
			TestCase{
				Name:      name,
				Primary:   pCtx,
				Secondary: sCtx,
				Test: func(combined context.Context) {
					if combined == nil || combined == pCtx || combined == sCtx {
						panic("unexpected combined")
					}
					if combined.Value("primary") != 1 {
						panic("primary value not set")
					}
					if combined.Value("secondary") != nil {
						panic("secondary value set")
					}
					if combined.Err() != nil {
						panic("expected not canceled")
					}
					pCancel()
					if combined.Err() == nil {
						panic("expected canceled")
					}
				},
			},
		)
	}()

	func() {
		var (
			name       = `secondary_cancel`
			pCtx, sCtx context.Context
			sCancel    context.CancelFunc
		)

		pCtx = context.Background()
		sCtx, sCancel = context.WithCancel(context.Background())
		pCtx = context.WithValue(pCtx, "primary", 1)
		sCtx = context.WithValue(sCtx, "secondary", 2)

		testCases = append(
			testCases,
			TestCase{
				Name:      name,
				Primary:   pCtx,
				Secondary: sCtx,
				Test: func(combined context.Context) {
					if combined == nil || combined == pCtx || combined == sCtx {
						panic("unexpected combined")
					}
					if combined.Value("primary") != 1 {
						panic("primary value not set")
					}
					if combined.Value("secondary") != nil {
						panic("secondary value set")
					}
					if combined.Err() != nil {
						panic("expected not canceled")
					}
					sCancel()
					<-combined.Done()
				},
			},
		)
	}()

	for i, testCase := range testCases {
		name := fmt.Sprintf(`TestCombineContext_pairs_#%d_%s`, i, testCase.Name)
		func() {
			defer func() {
				r := recover()
				if r != nil {
					t.Errorf(`%s failed: %+v`, name, r)
				}
			}()
			testCase.Test(CombineContext(testCase.Primary, testCase.Secondary))
		}()
	}
}

func TestCombineContext_nilOthersDoesNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	t.Cleanup(checkNumGoroutines(t))

	type ctxKey struct{}
	ctxVal := new(int)
	pretendCustomContext := context.WithValue(&valuelessContext{Context: ctx}, ctxKey{}, ctxVal)

	result := CombineContext(pretendCustomContext, nil, nil, nil, nil, nil, nil, nil)

	if result != pretendCustomContext {
		t.Error("expected CombineContext to return the first context if all others are nil")
	}
}

func TestCombineContext_anyCanceledExitsEarly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	t.Cleanup(checkNumGoroutines(t))

	type ctxKey struct{}
	ctxVal := new(int)
	pretendCustomContext := context.WithValue(&valuelessContext{Context: ctx}, ctxKey{}, ctxVal)

	canceledContext, canceledContextCancel := context.WithCancel(context.Background())
	canceledContextCancel()

	result := CombineContext(pretendCustomContext, nil, nil, nil, nil, &valuelessContext{Context: canceledContext}, nil, nil)

	if result == pretendCustomContext {
		t.Error(`expected a different context than the input`)
	}

	if err := result.Err(); err != context.Canceled {
		t.Error(`unexpected context error:`, err)
	}

	if v := result.Value(ctxKey{}); v != ctxVal {
		t.Error(`unexpected context value:`, v)
	}
}

func TestCombineContext_otherContextCancels_extra0(t *testing.T) {
	testCombineContextOtherContextCancels(t, 0)
}

func TestCombineContext_otherContextCancels_extra1(t *testing.T) {
	testCombineContextOtherContextCancels(t, 1)
}

func TestCombineContext_otherContextCancels_extra2(t *testing.T) {
	testCombineContextOtherContextCancels(t, 2)
}

func TestCombineContext_otherContextCancels_extra3(t *testing.T) {
	testCombineContextOtherContextCancels(t, 3)
}

func testCombineContextOtherContextCancels(t *testing.T, extra int) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	otherContext, otherContextCancel := context.WithCancel(context.Background())
	t.Cleanup(otherContextCancel)

	extras := make([]context.Context, 5)
	for i := range min(len(extras), extra) {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		extras[i] = &valuelessContext{Context: ctx}
	}

	t.Cleanup(checkNumGoroutines(t))

	type ctxKey struct{}
	ctxVal := new(int)
	pretendCustomContext := context.WithValue(&valuelessContext{Context: ctx}, ctxKey{}, ctxVal)

	goroutinesBefore := runtime.NumGoroutine()
	result := CombineContext(pretendCustomContext, nil, nil, extras[0], nil, &valuelessContext{Context: otherContext}, extras[1], extras[2])
	goroutinesAfter := runtime.NumGoroutine()
	t.Log(`goroutines before:`, goroutinesBefore, `after:`, goroutinesAfter)
	// the 2 are waiting for ctx and otherContext
	if expected := goroutinesBefore + 2 + extra; goroutinesAfter != expected {
		t.Error(`unexpected number of goroutines, expected:`, expected, `got:`, goroutinesAfter)
	}

	if result == pretendCustomContext {
		t.Error(`expected a different context than the input`)
	}

	if err := result.Err(); err != nil {
		t.Error(`unexpected context error:`, err)
	}

	if v := result.Value(ctxKey{}); v != ctxVal {
		t.Error(`unexpected context value:`, v)
	}

	done := result.Done()

	time.Sleep(time.Millisecond * 20)

	select {
	case <-done:
		t.Error(`expected result not yet canceled`)
	default:
	}

	otherContextCancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal(`timeout waiting for done channel`)
	}

	if err := result.Err(); err != context.Canceled {
		t.Error(`unexpected context error:`, err)
	}

	if result.Done() != done {
		t.Error(`unexpected done channel`)
	}
}

func TestCombineContext_noExtraGoroutinesWhenNotCustomContext(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	type ctxKey struct{}
	ctxVal := new(int)
	ctx = context.WithValue(ctx, ctxKey{}, ctxVal)

	extras := make([]context.Context, 2)
	for i := range extras {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		extras[i] = context.WithValue(ctx, ctxKey{}, i)
	}

	goroutinesBefore := runtime.NumGoroutine()
	result := CombineContext(ctx, nil, nil, extras[0], nil, extras[1], nil)
	goroutinesAfter := runtime.NumGoroutine()
	t.Log(`goroutines before:`, goroutinesBefore, `after:`, goroutinesAfter)
	if goroutinesBefore != goroutinesAfter {
		t.Error(`unexpected number of goroutines, expected:`, goroutinesBefore, `got:`, goroutinesAfter)
	}

	if result == ctx {
		t.Error(`expected a different context than the input`)
	}

	if err := result.Err(); err != nil {
		t.Error(`unexpected context error:`, err)
	}

	if v := result.Value(ctxKey{}); v != ctxVal {
		t.Error(`unexpected context value:`, v)
	}

	done := result.Done()

	time.Sleep(time.Millisecond * 20)

	select {
	case <-done:
		t.Error(`expected result not yet canceled`)
	default:
	}

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal(`timeout waiting for done channel`)
	}

	if err := result.Err(); err != context.Canceled {
		t.Error(`unexpected context error:`, err)
	}

	if result.Done() != done {
		t.Error(`unexpected done channel`)
	}
}

package bigbuff

import (
	"context"
	"testing"
	"fmt"
	"runtime"
	"time"
)

func TestCombineContext_each(t *testing.T) {
	initialGoroutineCount := runtime.NumGoroutine()

	for count := 1; count <= 100; count++ {
		for index := 0; index < count; index++ {
			var (
				others      = make([]context.Context, count)
				indexCancel context.CancelFunc
			)
			for i := 0; i < count; i++ {
				ctx, cancel := context.WithCancel(context.Background())
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
	initialGoroutineCount := runtime.NumGoroutine()

	for count := 2; count <= 20; count++ {
		for index := 0; index < count; index++ {
			var (
				others      = make([]context.Context, count)
				indexCancel context.CancelFunc
			)
			for i := 0; i < count; i++ {
				ctx, cancel := context.WithCancel(context.Background())
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

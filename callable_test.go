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
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// ExampleCall_rpc provides a (contrived) example of how Call may be used as part of an RPC implementation
func ExampleCall_rpc() {
	defer checkNumGoroutines(nil)

	var (
		methods = map[string]Callable{
			`add`: NewCallable(func(a, b int) int { return a + b }),
			`sum`: NewCallable(func(values ...int) (r int) {
				for _, v := range values {
					r += v
				}
				return
			}),
			`bounds`: NewCallable(func(values ...int) (min, max int, ok bool) {
				for _, value := range values {
					if !ok {
						min, max, ok = value, value, true
					} else {
						if value < min {
							min = value
						}
						if value > max {
							max = value
						}
					}
				}
				return
			}),
		}
		call = func(name string, args ...interface{}) (results []interface{}) {
			MustCall(
				methods[name],
				CallArgs(args...),
				CallResultsSlice(&results),
			)
			return
		}
		p = func(name string, args ...interface{}) {
			results := func() (v interface{}) {
				defer func() {
					if v == nil {
						v = recover()
					}
				}()
				v = call(name, args...)
				return
			}()
			fmt.Printf("%s %v -> %v\n", name, args, results)
		}
	)

	fmt.Println(`success:`)
	p(`add`, 1, 2)
	p(`bounds`, -123884, 4737, 9, 0, -99999992, 4, 6, 8324884383, -3)
	p(`bounds`)
	p(`bounds`, 2)
	p(`sum`, -123884, 4737, 9, 0, -99999992, 4, 6, 8324884383, -3)

	fmt.Println(`failure:`)
	p(`add`, 1, 2, 3)
	p(`add`, 1, 2.0)
	p(`bounds`, 2.0)

	//output:
	//success:
	//add [1 2] -> [3]
	//bounds [-123884 4737 9 0 -99999992 4 6 8324884383 -3] -> [-99999992 8324884383 true]
	//bounds [] -> [0 0 false]
	//bounds [2] -> [2 2 true]
	//sum [-123884 4737 9 0 -99999992 4 6 8324884383 -3] -> [8224765260]
	//failure:
	//add [1 2 3] -> bigbuff.CallArgs args error: invalid length: mandatory=2 variadic=false len=3
	//add [1 2] -> bigbuff.CallArgs args[1] error: float64 not assignable to int
	//bounds [2] -> bigbuff.CallArgs args[0] error: float64 not assignable to int
}

func TestNewCallable_notFunc(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	for i, v := range []interface{}{
		1,
		nil,
		new(func()),
	} {
		t.Run(fmt.Sprintf(`t%d`, i+1), func(t *testing.T) {
			var ok bool
			defer func() {
				if ok {
					t.Error(`expected panic`)
				} else if r := fmt.Sprint(recover()); r != fmt.Sprintf(`bigbuff.NewCallable not func: %v`, reflect.ValueOf(v).Kind()) {
					t.Error(r)
				}
			}()
			NewCallable(v)
			ok = true
		})
	}
}

func TestNewCallable_nilValue(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	for i, v := range []interface{}{
		(func())(nil),
		(func(bool) bool)(nil),
	} {
		t.Run(fmt.Sprintf(`t%d`, i+1), func(t *testing.T) {
			var ok bool
			defer func() {
				if ok {
					t.Error(`expected panic`)
				} else if r := fmt.Sprint(recover()); r != `bigbuff.NewCallable nil func` {
					t.Error(r)
				}
			}()
			NewCallable(v)
			ok = true
		})
	}
}

func ExampleCallArgs_funcResults() {
	defer checkNumGoroutines(nil)

	MustCall(
		NewCallable(fmt.Println),
		CallArgs(func() (int, string, bool) { return 3, `multiple return values -> varargs`, true }()),
	)
	//output:
	//3 multiple return values -> varargs true
}

func TestCallArgs_funcResults(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var output string
	MustCall(
		NewCallable(func(a int, b interface{}, c bool) error {
			output = strings.TrimSpace(fmt.Sprintln(a, b, c))
			return errors.New(`some error ignored`)
		}),
		CallArgs(func() (int, string, bool) { return 3, `multiple return values -> varargs`, true }()),
	)
	if output != `3 multiple return values -> varargs true` {
		t.Error(output)
	}
}

func TestCallable_Call_noArgs(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var count uint32
	MustCall(NewCallable(func(values ...fmt.Stringer) error {
		if len(values) != 0 {
			t.Error(values)
		}
		atomic.AddUint32(&count, 1)
		return nil
	}))
	if v := atomic.LoadUint32(&count); v != 1 {
		t.Error(v)
	}
}

func TestCallable_Call_noArgsButNonNilCallArgs(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var count uint32
	MustCall(
		NewCallable(func(values ...fmt.Stringer) error {
			if len(values) != 0 {
				t.Error(values)
			}
			atomic.AddUint32(&count, 1)
			return nil
		}),
		CallArgs(),
	)
	if v := atomic.LoadUint32(&count); v != 1 {
		t.Error(v)
	}
}

func TestCallable_Call_noArgsButNonNilCallArgsRaw(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var count uint32
	MustCall(
		NewCallable(func(values ...fmt.Stringer) error {
			if len(values) != 0 {
				t.Error(values)
			}
			if v := atomic.AddUint32(&count, 1); v != 2 {
				t.Error(v)
			}
			return nil
		}),
		CallArgsRaw(func() {
			if v := atomic.AddUint32(&count, 1); v != 1 {
				t.Error(v)
			}
		}),
	)
	if v := atomic.LoadUint32(&count); v != 2 {
		t.Error(v)
	}
}

func TestCallable_Call_argsHasIn(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	if err := Call(
		NewCallable(func(values ...fmt.Stringer) error { panic(`unexpected call`) }),
		CallArgsRaw(func([]int) int { panic(`unexpected args`) }),
	); err == nil || err.Error() != `bigbuff.callable args error: mandatory input` {
		t.Error(err)
	}
}

func TestCallable_Call_argsHasInVarargs(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	if err := Call(
		NewCallable(func(reader io.Reader) io.WriteCloser { return nil }),
		CallArgsRaw(func(...int) io.ReadCloser { return nil }),
		CallResultsRaw(func(io.Writer) int { return 3 }),
	); err != nil {
		t.Error(err)
	}
}

func TestCallable_Call_argsHasInVarargsAndMandatory(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	if err := Call(
		NewCallable(func(reader io.Reader) io.WriteCloser { return nil }),
		CallArgsRaw(func(int, ...int) io.ReadCloser { return nil }),
		CallResultsRaw(func(io.Writer) int { return 3 }),
	); err == nil || err.Error() != `bigbuff.callable args error: mandatory input` {
		t.Error(err)
	}
}

func TestCall_noOptions(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var (
		count uint32
		fn    = NewCallable(func() { atomic.AddUint32(&count, 1) })
	)
	MustCall(fn)
	if v := atomic.LoadUint32(&count); v != 1 {
		t.Fatal(v)
	}
	MustCall(fn)
	if v := atomic.LoadUint32(&count); v != 2 {
		t.Fatal(v)
	}
	MustCall(fn)
	if v := atomic.LoadUint32(&count); v != 3 {
		t.Fatal(v)
	}
}

func TestCallable_Call_invalidArgs(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	if err := Call(
		NewCallable(func() {}),
		CallArgsRaw(1),
	); err == nil || err.Error() != `bigbuff.callable args error: not func: int` {
		t.Error(err)
	}
}

func TestCallable_Call_nilArgs(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	if err := Call(
		NewCallable(func() {}),
		CallArgsRaw((func())(nil)),
	); err == nil || err.Error() != `bigbuff.callable args error: nil func` {
		t.Error(err)
	}
}

func TestCallable_Call_invalidResults(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	if err := Call(
		NewCallable(func() {}),
		CallResultsRaw(1),
	); err == nil || err.Error() != `bigbuff.callable results error: not func: int` {
		t.Error(err)
	}
}

func TestCallable_Call_nilResults(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	if err := Call(
		NewCallable(func() {}),
		CallResultsRaw((func())(nil)),
	); err == nil || err.Error() != `bigbuff.callable results error: nil func` {
		t.Error(err)
	}
}

func TestCallResultsSlice_noResults(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var (
		results []float64
		c       = NewCallable(func() {})
		o       = CallResultsSlice(&results)
	)
	MustCall(c, o)
	if results != nil {
		t.Error(results)
	}
	MustCall(NewCallable(func() {}), CallResultsSlice(&results))
	if results != nil {
		t.Error(results)
	}
	MustCall(c, o)
	if results != nil {
		t.Error(results)
	}
}

func TestCallResultsSlice_repeatedCalls(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var (
		results []fmt.Stringer
		fn      = func() (a time.Time, b fmt.Stringer, c *strings.Builder) {
			a = time.Unix(0, 18123883).UTC()
			b = a.Add(time.Hour)
			c = new(strings.Builder)
			c.WriteString(`pls`)
			return
		}
		c = NewCallable(fn)
		o = CallResultsSlice(&results)
	)
	MustCall(c, o)
	if len(results) != 3 {
		t.Error(results)
	}
	MustCall(NewCallable(fn), CallResultsSlice(&results))
	if len(results) != 6 {
		t.Error(results)
	}
	MustCall(c, o)
	if len(results) != 9 {
		t.Error(results)
	}
	if v := fmt.Sprint(results); v != "[1970-01-01 00:00:00.018123883 +0000 UTC 1970-01-01 01:00:00.018123883 +0000 UTC pls 1970-01-01 00:00:00.018123883 +0000 UTC 1970-01-01 01:00:00.018123883 +0000 UTC pls 1970-01-01 00:00:00.018123883 +0000 UTC 1970-01-01 01:00:00.018123883 +0000 UTC pls]" {
		t.Errorf(`%q`, v)
	}
}

type mockCallable struct {
	Callable
	t func() reflect.Type
}

func (m *mockCallable) Type() reflect.Type { return m.t() }

func TestCall_notFunc(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	if err := Call(&mockCallable{t: func() reflect.Type { return reflect.TypeOf(1) }}); err == nil || err.Error() != `bigbuff.Call type error: not func: int` {
		t.Error(err)
	}
}

func TestCallResults_noResults(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	if err := Call(NewCallable(func() {}), CallResults()); err != nil {
		t.Error(err)
	}
}

func TestCallResults_lessResults(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var a, b int
	if err := Call(NewCallable(func() (a, b, c int) {
		return
	}), CallResults(&a, &b)); err == nil || err.Error() != `bigbuff.CallResults results error: invalid length: mandatory=3 len=2` {
		t.Error(err)
	}
}

func TestCallResults_correctResults(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var (
		a, b int
		c    interface{}
	)
	if err := Call(NewCallable(func() (a, b, c int) {
		return
	}), CallResults(&a, &b, &c)); err != nil {
		t.Error(err)
	}
}

func TestCallResults_nonPointer(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	if err := Call(NewCallable(func() (int, int) { return 3, 3 }), CallResults(new(int), 0)); err == nil || err.Error() != `bigbuff.CallResults results[1] error: int kind int not ptr` {
		t.Error(err)
	}
}

func TestCallResults_nilPointer(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	if err := Call(NewCallable(func() int { return 3 }), CallResults((*int)(nil))); err == nil || err.Error() != `bigbuff.CallResults results[0] error: *int value is nil` {
		t.Error(err)
	}
}

func TestCallResults_notAssignable(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var out io.ReadCloser
	if err := Call(NewCallable(func() io.Reader { return nil }), CallResults(&out)); err == nil || err.Error() != `bigbuff.CallResults results[0] error: io.Reader not assignable to io.ReadCloser` {
		t.Error(err)
	}
}

func ExampleCallable_Call_results() {
	defer checkNumGoroutines(nil)

	if err := NewCallable(func() (bool, int, []string) { return true, 6, []string{`a`, `b`} }).Call(nil, fmt.Println); err != nil {
		panic(err)
	}
	//output:
	//true 6 [a b]
}

func TestCallResultsSlice_notPtr(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	if err := Call(NewCallable(func() int { return 3 }), CallResultsSlice([]int{})); err == nil || err.Error() != `bigbuff.CallResultsSlice target error: not ptr: []int` {
		t.Error(err)
	}
}

func TestCallResultsSlice_nilPtr(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	if err := Call(NewCallable(func() int { return 3 }), CallResultsSlice((*[]int)(nil))); err == nil || err.Error() != `bigbuff.CallResultsSlice target error: nil ptr: *[]int` {
		t.Error(err)
	}
}

func TestCallResultsSlice_notSlice(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	if err := Call(NewCallable(func() int { return 3 }), CallResultsSlice(new(bool))); err == nil || err.Error() != `bigbuff.CallResultsSlice target error: not slice: *bool` {
		t.Error(err)
	}
}

func TestCallResultsSlice_notAssignable(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	if err := Call(NewCallable(func() io.Writer { return nil }), CallResultsSlice(new([]io.WriteCloser))); err == nil || err.Error() != `bigbuff.CallResultsSlice results[0] error: io.Writer not assignable to io.WriteCloser` {
		t.Error(err)
	}
}

func TestCallResultsSlice_assignable(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	if err := Call(NewCallable(func() io.WriteCloser { return nil }), CallResultsSlice(new([]io.Writer))); err != nil {
		t.Error(err)
	}
}

type callPassTester struct {
	config   callConfig
	validate func(x callConfig)
}

func callPass(args, results []interface{}) CallOption {
	opts := []CallOption{
		CallArgs(args...),
		CallResults(results...),
	}
	return func(config *callConfig) error {
		for _, opt := range opts {
			if err := opt(config); err != nil {
				return err
			}
		}
		return nil
	}
}

func (x callPassTester) f1(a int, b *bool, c interface{}, d fmt.Stringer) (e string, f *int, g interface{}, h io.Reader) {
	x.config.this = reflect.TypeOf(x.f1)
	if err := callPass([]interface{}{a, b, c, d}, []interface{}{&e, &f, &g, &h})(&x.config); err != nil {
		panic(err)
	}
	x.validate(x.config)
	return
}

func Test_callPass_f1(t *testing.T) {
	t.Cleanup(checkNumGoroutines(t))

	var (
		a, b, c, d                         = 123, true, new(bytes.Buffer), new(strings.Builder)
		e, f, g, h                         = `456`, 789, make(chan struct{}), new(bytes.Reader)
		actualA                            int
		actualB                            *bool
		actualC                            interface{}
		actualD                            fmt.Stringer
		actualE, actualF, actualG, actualH = (callPassTester{validate: func(config callConfig) {
			if fn, ok := config.args.(func() (int, *bool, interface{}, fmt.Stringer)); !ok {
				t.Errorf(`%T`, config.args)
			} else {
				actualA, actualB, actualC, actualD = fn()
			}
			if fn, ok := config.results.(func(string, *int, interface{}, io.Reader)); !ok {
				t.Errorf(`%T`, config.results)
			} else {
				fn(e, &f, g, h)
			}
		}}).f1(a, &b, c, d)
	)
	if actualA != a {
		t.Error(actualA)
	}
	if actualB != (&b) {
		t.Error(actualB)
	}
	if actualC != c {
		t.Error(actualC)
	}
	if actualD != d {
		t.Error(actualD)
	}
	if actualE != e {
		t.Error(actualE)
	}
	if actualF != (&f) {
		t.Error(actualF)
	}
	if actualG != g {
		t.Error(actualG)
	}
	if actualH != h {
		t.Error(actualH)
	}
}

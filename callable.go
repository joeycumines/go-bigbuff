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
	"fmt"
	"reflect"
)

type (
	// Callable models a function, and is used by this package to provide a higher-level mechanism (than the reflect
	// package) for calling arbitrary functions, in a generic way, see also the NewCallable factory function
	Callable interface {
		// Type must return a valid type of kind func, corresponding to the type of the Callable
		Type() reflect.Type
		// Call accepts an args function and passes results into a results function, to be utilised in a manner
		// generally equivalent to `results(callable(args()))`, treating nil (interface values) as omitting any
		// args, or not handling any return value. Note that Call will return an error if args or results are not
		// compatible with the underlying types, indicated by Callable.Type. See also bigbuff.Call and CallOption.
		Call(args, results interface{}) error
	}

	// CallOption models a configuration option for the Call function provided by this package, see also the
	// Call-prefixed functions provided by this package
	CallOption func(config *callConfig) error

	// callConfig models configuration for the Call function
	callConfig struct {
		this    reflect.Type
		args    interface{}
		results interface{}
	}

	callable struct {
		callableValue
	}
	callableValue interface {
		Type() reflect.Type
		Call(in []reflect.Value) []reflect.Value
	}
)

// NewCallable initialises a new Callable from fn, which must be a non-nil function, but is otherwise unconstrained,
// note a panic will occur if fn is not a function, or is a function but isn't non-nil
func NewCallable(fn interface{}) Callable {
	v := reflect.ValueOf(fn)
	if kind := v.Kind(); kind != reflect.Func {
		panic(fmt.Errorf(`bigbuff.NewCallable not func: %v`, kind))
	}
	if v.IsNil() {
		panic(fmt.Errorf(`bigbuff.NewCallable nil func`))
	}
	return &callable{v}
}

// Call will call a Callable with any provided options, available as functions provided by this package that are
// prefixed with "Call", see also MustCall
func Call(caller Callable, options ...CallOption) error {
	config := &callConfig{this: caller.Type()}
	if kind := config.this.Kind(); kind != reflect.Func {
		return fmt.Errorf(`bigbuff.Call type error: not func: %v`, kind)
	}
	for _, option := range options {
		if err := option(config); err != nil {
			return err
		}
	}
	return caller.Call(config.args, config.results)
}

// MustCall is equivalent to Call but will panic on error
func MustCall(caller Callable, options ...CallOption) {
	if err := Call(caller, options...); err != nil {
		panic(err)
	}
}

// CallArgs returns a CallOption that will pass the provided args to a Callable that is called via the Call function
func CallArgs(args ...interface{}) CallOption {
	return func(config *callConfig) error {
		in, err := resolveArgs(config.this, typesArgs(args))
		if err != nil {
			return fmt.Errorf(`bigbuff.CallArgs %s`, err)
		}
		config.args = reflect.MakeFunc(
			reflect.FuncOf(nil, in, false),
			func([]reflect.Value) (results []reflect.Value) {
				results = make([]reflect.Value, len(in))
				for i, in := range in {
					results[i] = reflect.New(in).Elem()
					results[i].Set(reflect.ValueOf(args[i]))
				}
				return
			},
		).Interface()
		return nil
	}
}

// CallResults returns a CallOption that will assign the call's results to the values pointed at by results
func CallResults(results ...interface{}) CallOption {
	return func(config *callConfig) error {
		out := typesInOut(config.this.NumOut(), config.this.Out)
		if len(results) != len(out) {
			return fmt.Errorf(`bigbuff.CallResults results error: invalid length: mandatory=%d len=%d`, len(out), len(results))
		}
		for i, out := range out {
			v := reflect.ValueOf(results[i])
			t := v.Type()
			if kind := t.Kind(); kind != reflect.Ptr {
				return fmt.Errorf(`bigbuff.CallResults results[%d] error: %v kind %v not ptr`, i, t, kind)
			}
			if v.IsNil() {
				return fmt.Errorf(`bigbuff.CallResults results[%d] error: %v value is nil`, i, t)
			}
			t = t.Elem()
			if !out.AssignableTo(t) {
				return fmt.Errorf(`bigbuff.CallResults results[%d] error: %v not assignable to %v`, i, out, t)
			}
		}
		config.results = reflect.MakeFunc(
			reflect.FuncOf(out, nil, false),
			func(args []reflect.Value) []reflect.Value {
				for i, arg := range args {
					reflect.ValueOf(results[i]).Elem().Set(arg)
				}
				return nil
			},
		).Interface()
		return nil
	}
}

// CallResultsSlice returns a CallOption that will append the call's results to a slice pointed at by target
func CallResultsSlice(target interface{}) CallOption {
	return func(config *callConfig) error {
		value := reflect.ValueOf(target)
		if value.Kind() != reflect.Ptr {
			return fmt.Errorf(`bigbuff.CallResultsSlice target error: not ptr: %T`, target)
		}
		if value.IsNil() {
			return fmt.Errorf(`bigbuff.CallResultsSlice target error: nil ptr: %T`, target)
		}
		if value.Elem().Kind() != reflect.Slice {
			return fmt.Errorf(`bigbuff.CallResultsSlice target error: not slice: %T`, target)
		}
		out := typesInOut(config.this.NumOut(), config.this.Out)
		{
			elem := value.Elem().Type().Elem()
			for i, v := range out {
				if !v.AssignableTo(elem) {
					return fmt.Errorf(`bigbuff.CallResultsSlice results[%d] error: %v not assignable to %v`, i, v, elem)
				}
				out[i] = elem
			}
		}
		config.results = reflect.MakeFunc(
			reflect.FuncOf(out, nil, false),
			func(args []reflect.Value) []reflect.Value {
				if len(args) != 0 {
					value.Elem().Set(reflect.Append(value.Elem(), args...))
				}
				return nil
			},
		).Interface()
		return nil
	}
}

// CallArgsRaw returns a CallOption that will pass args to Callable.Call without modification or validation
func CallArgsRaw(args interface{}) CallOption {
	return func(config *callConfig) error {
		config.args = args
		return nil
	}
}

// CallResultsRaw returns a CallOption that will pass results to Callable.Call without modification or validation
func CallResultsRaw(results interface{}) CallOption {
	return func(config *callConfig) error {
		config.results = results
		return nil
	}
}

// Call implements Callable.Call using the reflect package
func (x *callable) Call(args, results interface{}) error {
	var argsV reflect.Value
	if args != nil {
		argsV = reflect.ValueOf(args)
		if kind := argsV.Kind(); kind != reflect.Func {
			return fmt.Errorf(`bigbuff.callable args error: not func: %v`, kind)
		}
		if argsV.IsNil() {
			return fmt.Errorf(`bigbuff.callable args error: nil func`)
		}
		if n := argsV.Type().NumIn(); n != 0 && (n != 1 || !argsV.Type().IsVariadic()) {
			return fmt.Errorf(`bigbuff.callable args error: mandatory input`)
		}
	}

	var resultsV reflect.Value
	if results != nil {
		resultsV = reflect.ValueOf(results)
		if kind := resultsV.Kind(); kind != reflect.Func {
			return fmt.Errorf(`bigbuff.callable results error: not func: %v`, kind)
		}
		if resultsV.IsNil() {
			return fmt.Errorf(`bigbuff.callable results error: nil func`)
		}
	}

	var in []reflect.Value
	if argsV != (reflect.Value{}) {
		in = argsV.Call(nil)
	}

	out := x.callableValue.Call(in)

	if resultsV != (reflect.Value{}) {
		resultsV.Call(out)
	}

	return nil
}

func typesInOut(n int, fn func(i int) reflect.Type) []reflect.Type {
	r := make([]reflect.Type, n)
	for i := range r {
		r[i] = fn(i)
	}
	return r
}

func typesArgs(args []interface{}) []reflect.Type {
	r := make([]reflect.Type, len(args))
	for i, arg := range args {
		r[i] = reflect.TypeOf(arg)
	}
	return r
}

func resolveArgs(this reflect.Type, args []reflect.Type) ([]reflect.Type, error) {
	var (
		in       = typesInOut(this.NumIn(), this.In)
		variadic reflect.Type
	)
	if this.IsVariadic() {
		variadic, in[len(in)-1], in = in[len(in)-1].Elem(), nil, in[:len(in)-1]
		for len(args) > len(in) {
			in = append(in, variadic)
		}
	}
	if len(args) != len(in) {
		return nil, fmt.Errorf(`args error: invalid length: mandatory=%d variadic=%v len=%d`, len(in), variadic != nil, len(args))
	}
	for i, in := range in {
		if !args[i].AssignableTo(in) {
			return nil, fmt.Errorf(`args[%d] error: %v not assignable to %v`, i, args[i], in)
		}
	}
	return in, nil
}

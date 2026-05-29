/*
   Copyright 2026 Joseph Cumines

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
	"iter"
)

// Seq provides a sequence of values from the given [Getter].
func Seq[T any](ctx context.Context, source Getter[T]) iter.Seq2[T, error] {
	if ctx == nil {
		panic("bigbuff.Seq requires non-nil ctx")
	}
	if source == nil {
		panic("bigbuff.Seq requires non-nil source")
	}
	return func(yield func(T, error) bool) {
		err := ctx.Err()
		var value T
		if err == nil {
			for value, err = source.Get(ctx); ; value, err = source.Get(ctx) {
				if err != nil {
					break
				}
				if !yield(value, nil) {
					return
				}
			}
		}
		yield(*new(T), err)
	}
}

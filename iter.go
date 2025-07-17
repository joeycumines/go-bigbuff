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

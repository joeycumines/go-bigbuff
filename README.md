# go-bigbuff

> test coverage: 97.1%

Package bigbuff implements many useful concurrency primitives and utilities. It was originally created around
`bigbuff.Buffer`, a one-to-many unbounded FIFO queue with a built in retention policy mechanism, and two interfaces
`bigbuff.Consumer`, and `bigbuff.Producer`, which generalise the pattern in a way that supports complex use cases such
as at-least-once semantics. Over time this package has collected a range of different, often highly specialised
implementations, that deal with different aspects of the complicated challenge of concurrent programming.

Godoc here: [github.com/joeycumines/go-bigbuff](https://godoc.org/github.com/joeycumines/go-bigbuff)

Specialised tools should be carefully considered against possible use cases, and those provided by this package are no
different. A good rule of thumb is to keep them peripheral and replaceable, where possible. Choose wisely ;).

## Highlights

- `bigbuff.Buffer` is the most mature implementation in this package, and is battle tested one-many producer / consumer
   implementation. It operates as an unbounded FIFO queue by default, reaping messages after they are read by all (and
   at least one) consumer. Custom behavior may be implemented using the `bigbuff.Cleaner` type. The buffer's behavior
   may be modified to enforce (soft) bounding of the size of the queue, via the `bigbuff.FixedBufferCleaner` function.
   Benchmarks... someday.
- Any readable channel may be used as a `bigbuff.Consumer`, using `bigbuff.NewChannel`
- `bigbuff.Notifier` uses `reflect.Select` to provide synchronous fan-out pub-sub (keyed, sending to any supported
  channel that is subscribed at time of publish). This implementation is far more compact than `bigbuff.Buffer`, but
  at this time it is not actively being used in a production environment. It is also far slower, and suitable for a
  much smaller number of concurrent consumers (per key / buffer instance). That said it is much easier to use, and
  solves my original problem case of dynamically wiring up existing message processing that makes heavy use of channels
  very well. The learning curve for this implementation should be trivial, as it's behavior is identical to direct
  interaction with multiple channels + context, using select.
- `bigbuff.Exclusive` is another battle-tested implementation, providing debouncing of _keyed operations_, either as
  part of a background process, or in the foreground, potentially blocking for a return value (which may be shared
  between multiple, debounced callers)
- Useful for limiting concurrent executions, `bigbuff.Workers` is compatible with `bigbuff.Exclusive`. It's a simple
  on-demand background worker orchestrator, which deliberately uses a compatible function signature with relevant
  methods from `bigbuff.Exclusive`. `bigbuff.MinDuration` is provided in the same vein.

## Caveats

- All implementations were learning experiences, and in some, particularly older cases, the API has been left unchanged
  solely for backwards compatibility
- Unbounded queues tend to require additional management, especially if there is any chance that the consumer side may
  fall behind the producer side (cascading failures == bad)
- The use cases of most of these tools are complicated enough to deserve extensive examples and discussion

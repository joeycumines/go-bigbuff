# go-bigbuff

> test coverage: 93.7%

Package bigbuff provides an in-memory `Buffer` implementation, which is designed to support a `Producer`-`Consumer`
(one to many) pattern, where each consumer reads the full data set, and has their own offsets, with atomic
commits and rollbacks. It also provides a well designed native channel backed `Consumer` implementation, that
provides the same kind of feature set, but with is geared towards fan-in implementations, with the same benefits
on the consumer end as provided by the `Buffer` implementation, also solving channel draining and "zero reads"
on producer channel close (at the cost of some performance vs pure channels, of course). There are also some
utility helper methods which are exposed as part of this package.

Godoc here: [github.com/joeycumines/go-bigbuff](https://godoc.org/github.com/joeycumines/go-bigbuff)

Features:
 - Queue implementations with strong support for context.Context, aiming to guarantee as much data consistency
   as possible
 - Well defined interfaces designed after the style of core golang libs
 - Handy `Range` method for iterating, with catch / throw pattern for panics
 - Exposes async util functions to combine contexts and safely perform a conditional wait via sync.Cond (no leaks)
 - Configurable cleanup behavior - control retention based on the length and consumer offsets + the min cycle wait
 - The whole reason why I implemented this in the first place, a producer implementation that is 100% safe to use
   within a consumer processing loop (unbounded buffer size, e.g. imagine something ranging from a buffered
   channel but also sending to it, but it's 1 consumer, so as soon as the buffer fills, it will deadlock if it
   tries a send, and another consumer would break guaranteed in-order processing)
 - Single-channel implementation of the `bigbuff.Consumer` interface, that is uses reflection + polling to provide
   the same commit and rollback functionality (retains order, supports any readable channel).
 - Breaking from the mould a little `bigbuff.Exclusive` allows key based locking and de-bouncing for operations 
   that may return values like `(interface{}, error)`, operating synchronously to keep things simple
 - Useful for limiting concurrent executions `bigbuff.Workers` is compatible with `bigbuff.Exclusive`

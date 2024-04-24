package bigbuff

import (
	"math"
	"sync"
	"sync/atomic"
)

// ChanCaster supports broadcasting messages to multiple receivers, via a
// single channel, without callbacks.
//
// See also [NewChanCaster] (optional).
type ChanCaster[C chan V, V any] struct {
	// Channel will be used to broadcast messages.
	//
	// See [ChanCaster.Add] and [ChanCaster.Send] for usage details.
	C C

	// mutex synchronises [ChanCaster.Send] and [ChanCaster.Add] calls with
	// positive deltas. Ensuring sends are mutually exclusive is fairly
	// important for consistency, but not strictly necessary. The primary
	// purpose of this mutex is to avoid incrementing the number of receivers
	// while any send is in progress, to facilitate removing receivers while
	// send is in progress.
	mutex sync.RWMutex
	// state synchronises the number of receivers, between [ChanCaster.Send]
	// calls, and [ChanCaster.Add] calls with negative deltas.
	// The high 32 bits are number of receivers (modelled as int32), while the
	// low 32 bits are either identical, or [math.MaxInt32] + the number of
	// receivers being sent to (at the start of [ChanCaster.Send]), that
	// haven't been removed, via [ChanCaster.Add] with a negative delta.
	state atomic.Uint64
}

// NewChanCaster is a factory for [ChanCaster] that exists solely for the
// convenience of type inference.
func NewChanCaster[C chan V, V any](channel C) *ChanCaster[C, V] {
	return &ChanCaster[C, V]{C: channel}
}

// Send broadcasts a message, to all registered receivers, via
// [ChanCaster.C]. If the contract of [ChanCaster] is obeyed, this call will
// never block for any significant time.
//
// See [ChanCaster.Add] for usage details.
func (x *ChanCaster[C, V]) Send(value V) {
	if x.state.Load() == 0 {
		return // no receivers (fast path)
	}

	// prevent receivers being added while sending + order values by send call
	x.mutex.Lock()
	defer x.mutex.Unlock()

	// load our state, guard no receivers (early exit), and set tracker to the
	// value of `maxInt32 + receivers`, using CAS to sync with negative adds
	var receivers uint32
	{
		var (
			state   uint64
			tracker uint32
		)
		for {
			state = x.state.Load()
			if state == 0 {
				return // no receivers (slow path)
			}

			receivers = uint32(state >> 32) // initialize from hi
			tracker = uint32(state)         // initialize from lo
			if tracker != receivers || receivers > math.MaxInt32 {
				panic(`bigbuff: chancaster: send: state invariant violation`)
			}

			// attempt to set tracker to `maxInt32 + receivers`, with CAS used
			// to synchronise with decrements of receivers
			tracker += math.MaxInt32
			if x.state.CompareAndSwap(state, uint64(receivers)<<32+uint64(tracker)) {
				break
			}
		}
	}

	// broadcast involves sending to all receivers - with the total actually
	// received being in range [0, receivers], due to potential decrements
	for range receivers {
		x.C <- value // may end up received by negative Add calls
	}

	// reset state - easy, as we just broadcast, and no increments occurred
	x.state.Store(0)
}

// Add increments or decrements the number of receivers, for [ChanCaster.C].
//
// Each added receiver represents the intent to receive exactly one value, from
// [ChanCaster.C]. Each receiver removed (via a negative delta) represents
// exactly one added receiver, which has not yet been removed, and has not and
// will not receive a value. The valid range for the number of receivers is
// [0, math.MaxInt32], and any add which results in a number of receivers
// outside this range will cause a panic.
//
// The typical usage pattern is to call [ChanCaster.Add] with a delta of `1`,
// then immediately receive, or attempt to receive, e.g. within a `select`
// statement. If the next-received value (e.g. within said `select` statement)
// is not from [ChanCaster.C], and receive won't be promptly re-attempted, then
// [ChanCaster.Add] should be called again, with the inverse of the previous
// delta.
//
// Using a delta greater than `1` indicates multiple separate receivers, which
// will all receive the same value, from the same [ChanCaster.Send] call. These
// receivers should independently decrement the number of receivers, if
// necessary, as described above.
func (x *ChanCaster[C, V]) Add(delta int) {
	const maxReceivers = math.MaxInt32

	switch {
	case delta == 0:
		return

	case delta > 0:
		if delta > maxReceivers {
			panic(`bigbuff: chancaster: add: positive delta out of bounds`)
		}

		// inc needs read lock to be mutually exclusive with sends
		x.mutex.RLock()
		defer x.mutex.RUnlock()

		// add delta to both hi and lo
		state := x.state.Add(uint64(delta)<<32 + uint64(uint32(delta)))

		// validate to ensure we did not overflow + sanity check invariants
		if receivers := uint32(state >> 32); receivers <= maxReceivers &&
			receivers >= uint32(delta) &&
			receivers == uint32(state) {
			return
		}

	case delta < -maxReceivers:
		panic(`bigbuff: chancaster: add: negative delta out of bounds`)

	default:
		// flip delta to positive, from negative
		delta = -delta

		// note: same delta calc as above, subtracted using two's complement rules
		state := x.state.Add(^(uint64(delta)<<32 + uint64(uint32(delta)) - 1))

		receivers := uint32(state >> 32) // initialize from hi
		tracker := uint32(state)         // initialize from lo

		// validate, and, if necessary, receive any channel sends that would
		// otherwise never be received (to avoid Send hanging)
		if receivers <= maxReceivers {
			switch tracker {
			case receivers:
				return // not sending

			case maxReceivers + receivers:
				// sending - perform the requisite number of receives (however
				// many we decremented, or, at most, however many are
				// remaining, from those being sent)
				for range min(delta, int(tracker-maxReceivers)) {
					<-x.C
				}
				return
			}
		}
	}

	// invariant violation, e.g. due to OoB add, or previous violation
	panic(`bigbuff: chancaster: add: state invariant violation`)
}

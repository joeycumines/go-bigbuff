package bigbuff

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-test/deep"
	"runtime"
	"testing"
	"time"
)

func TestRange_oneConsumerSuccessCleanup(t *testing.T) {
	initialGoroutines := runtime.NumGoroutine()

	b := new(Buffer)

	pairs := make([]struct {
		Index int
		Value interface{}
	}, 0)

	b.Put(nil, -1, -2, -3)

	time.Sleep(time.Millisecond * 100)

	if s := b.Size(); s != 3 {
		t.Fatal("unexpected buffer size", s)
	}

	c, _ := b.NewConsumer()
	go func() {
		b.Put(nil, 1, 2, 3)
		time.Sleep(time.Millisecond * 10)
		b.Put(nil, 4, 5)
		time.Sleep(time.Millisecond * 40)
	}()

	if err := Range(nil, c, func(index int, value interface{}) bool {
		pairs = append(pairs, struct {
			Index int
			Value interface{}
		}{
			Index: index,
			Value: value,
		})
		return index != 6
	}); err != nil {
		t.Error("unexpected error", err)
	}

	time.Sleep(time.Millisecond * 100)

	if s := b.Size(); s != 1 {
		t.Error("unexpected buffer size", s)
	}

	if s := b.Slice(); len(s) != 1 || s[0] != 5 {
		t.Error("unexpected buffer", s)
	}

	if diff := deep.Equal([]struct {
		Index int
		Value interface{}
	}{
		{
			Index: 0,
			Value: -1,
		},
		{
			Index: 1,
			Value: -2,
		},
		{
			Index: 2,
			Value: -3,
		},
		{
			Index: 3,
			Value: 1,
		},
		{
			Index: 4,
			Value: 2,
		},
		{
			Index: 5,
			Value: 3,
		},
		{
			Index: 6,
			Value: 4,
		},
	}, pairs); diff != nil {
		t.Error("unexpected pairs diff:", diff)
	}

	if err := b.Close(); err != nil {
		t.Error("unexpected close error", err)
	}

	time.Sleep(time.Millisecond * 10)

	if s := b.Slice(); len(s) != 1 || s[0] != 5 {
		t.Error("unexpected buffer", s)
	}

	finalGoroutines := runtime.NumGoroutine()

	if finalGoroutines > initialGoroutines {
		t.Fatal("goroutine diff:", finalGoroutines-initialGoroutines)
	}
}

//func TestBuffer_fanInAndOut(t *testing.T) {
//	// TODO: delete this and replace it with something that makes sense
//
//	// iterate on a heavily loaded test multiple times, ensure the goroutine counts stay consistent
//	initialGoroutines := runtime.NumGoroutine()
//	var (
//		preStopGoroutines   []int
//		firstStopGoroutines []int
//		postStopGoroutines  []int
//		inputCount          = 5000
//		producerCount       = 10
//		consumerCount       = 10
//	)
//
//	for iterationX := 1; iterationX <= 10; iterationX++ {
//		buffer := new(Buffer)
//
//		for iterationY := 1; iterationY <= 10; iterationY++ {
//			// build inputs for multiple producers that are self - marking with a timestamp + consumer id
//
//			ID := 0
//			nextID := func() (nextID int) {
//				nextID = ID
//				ID++
//				return
//			}
//
//			// set this when starting to ns epoch
//			var startedAt int64
//
//			type (
//				InputData struct {
//					ID       int
//					Produced time.Duration
//					Consumed map[int]*[]time.Duration
//				}
//			)
//			var (
//				maxInputPerProducer = int(math.Ceil(float64(inputCount) / float64(producerCount)))
//				inputState          = make([]*InputData, inputCount)
//				producerMap         = make(map[int][]*InputData)
//				consumerMap         = make(map[int]Consumer)
//			)
//			for i := range inputState {
//				if i != nextID() {
//					panic("bad id?")
//				}
//				inputState[i] = &InputData{
//					ID:       i,
//					Consumed: make(map[int]*[]time.Duration),
//				}
//			}
//			inputIndex := 0
//			for x := 0; x < producerCount; x++ {
//				producerID := nextID()
//				producerMap[producerID] = make([]*InputData, 0, maxInputPerProducer)
//				for y := 0; y < maxInputPerProducer; y++ {
//					if inputIndex >= inputCount {
//						break
//					}
//					producerMap[producerID] = append(producerMap[producerID], inputState[inputIndex])
//					inputIndex++
//				}
//			}
//			for x := 0; x < consumerCount; x++ {
//				consumerID := nextID()
//				consumerMap[consumerID], _ = buffer.NewConsumer()
//				for i := range inputState {
//					inputState[i].Consumed[consumerID] = new([]time.Duration)
//				}
//			}
//
//			starter := new(sync.RWMutex)
//			starter.Lock()
//			wg := new(sync.WaitGroup)
//			wg.Add(producerCount + consumerCount)
//
//			for _, producerData := range producerMap {
//				func() {
//					producerData := producerData
//					go func() {
//						defer wg.Done()
//						starter.RLock()
//						defer starter.RUnlock()
//						for _, data := range producerData {
//							data.Produced = time.Duration(time.Now().UnixNano() - startedAt)
//							if err := buffer.Put(nil, data); err != nil {
//								panic(err)
//							}
//						}
//					}()
//				}()
//			}
//
//			for consumerID, consumer := range consumerMap {
//				func() {
//					consumerID, consumer := consumerID, consumer
//					go func() {
//						defer wg.Done()
//						starter.RLock()
//						defer starter.RUnlock()
//						count := 0
//						if err := Range(nil, consumer, func(index int, value interface{}) bool {
//							data := value.(*InputData)
//							consumed := data.Consumed[consumerID]
//							*consumed = append(*consumed, time.Duration(time.Now().UnixNano()-startedAt))
//							count++
//							return count != inputCount
//						}); err != nil {
//							panic(err)
//						}
//					}()
//				}()
//			}
//
//			time.Sleep(time.Millisecond * 10)
//			startedAt = time.Now().UnixNano()
//			starter.Unlock()
//			wg.Wait()
//
//			// ensure every consumer received every message in order relative to the producers
//			for consumerID := range consumerMap {
//				for _, producerData := range producerMap {
//					var lastTS time.Duration
//					for _, data := range producerData {
//						if len(*data.Consumed[consumerID]) != 1 {
//							t.Fatalf("unexpected consumed for id %d: %v", consumerID, *data.Consumed[consumerID])
//						}
//						ts := (*data.Consumed[consumerID])[0]
//						if ts <= lastTS {
//							t.Error("this ts (1) was not after last ts (2)", ts, lastTS)
//						}
//						lastTS = ts
//					}
//				}
//			}
//
//			// check each message
//			for _, data := range inputState {
//				if len(data.Consumed) != consumerCount {
//					t.Fatal("bad data", data)
//				}
//				for _, ts := range data.Consumed {
//					if len(*ts) != 1 {
//						t.Fatal("bad data", data)
//					}
//				}
//			}
//
//			time.Sleep(time.Millisecond * 60)
//
//			if l := len(buffer.buffer); 0 != l {
//				t.Fatal("bad len", l)
//			}
//
//			preStopGoroutines = append(preStopGoroutines, runtime.NumGoroutine())
//
//			for _, consumer := range consumerMap {
//				if err := consumer.Close(); err != nil {
//					t.Fatal(err)
//				}
//			}
//
//			time.Sleep(time.Millisecond * 50)
//
//			firstStopGoroutines = append(firstStopGoroutines, runtime.NumGoroutine())
//		}
//
//		if err := buffer.Close(); err != nil {
//			t.Fatal(err)
//		}
//
//		time.Sleep(time.Millisecond * 50)
//
//		postStopGoroutines = append(postStopGoroutines, runtime.NumGoroutine())
//	}
//
//	time.Sleep(time.Second)
//
//	finalGoroutines := runtime.NumGoroutine()
//
//	//fmt.Println(initialGoroutines)
//	//fmt.Println(preStopGoroutines)
//	//fmt.Println(firstStopGoroutines)
//	//fmt.Println(postStopGoroutines)
//	//fmt.Println(finalGoroutines)
//
//	if finalGoroutines != initialGoroutines {
//		t.Errorf("inital goroutines %d != final %d", initialGoroutines, finalGoroutines)
//	}
//
//	// compared with the actual counts - initial
//	maxAllowedPreStop := consumerCount * 6
//	maxAllowedFirstStop := 2
//	maxAllowedPostStop := 0
//
//	for _, preStop := range preStopGoroutines {
//		preStop -= initialGoroutines
//		if preStop > maxAllowedPreStop {
//			t.Fatal("bad pre stop", preStopGoroutines)
//		}
//	}
//
//	for _, firstStop := range firstStopGoroutines {
//		firstStop -= initialGoroutines
//		if firstStop > maxAllowedFirstStop {
//			t.Fatal("bad first stop", firstStopGoroutines)
//		}
//	}
//
//	for _, postStop := range postStopGoroutines {
//		postStop -= initialGoroutines
//		if postStop > maxAllowedPostStop {
//			t.Fatal("bad post stop", postStopGoroutines)
//		}
//	}
//}

func TestFixedBufferCleaner(t *testing.T) {
	out := make(chan FixedBufferCleanerNotification, 1)

	buffer := new(Buffer)
	defer buffer.Close()
	buffer.SetCleanerConfig(CleanerConfig{
		Cooldown: DefaultCleanerCooldown,
		Cleaner: FixedBufferCleaner(
			3,
			1,
			func(notification FixedBufferCleanerNotification) {
				out <- notification
			},
		),
	})

	consumer, err := buffer.NewConsumer()
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	defer consumer.Rollback()

	if err := buffer.Put(nil, 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond * 30)
	select {
	case <-out:
		t.Fatal("didn't expect")
	default:
	}

	// ensure that it can be consumed normally, and calls default logic
	if v, err := consumer.Get(nil); err != nil || v != 1 {
		t.Fatal("unexpected values", v, err)
	}
	if err := consumer.Commit(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond * 50)
	if buffer.Size() != 0 {
		t.Error("expected no size")
	}
	time.Sleep(time.Millisecond * 30)
	select {
	case <-out:
		t.Fatal("didn't expect")
	default:
	}

	if err := buffer.Put(nil, 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond * 30)
	select {
	case <-out:
		t.Fatal("didn't expect")
	default:
	}

	if err := buffer.Put(nil, 2); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond * 30)
	select {
	case <-out:
		t.Fatal("didn't expect")
	default:
	}

	if err := buffer.Put(nil, 3); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond * 30)
	select {
	case <-out:
		t.Fatal("didn't expect")
	default:
	}

	if diff := deep.Equal(buffer.Slice(), []interface{}{1, 2, 3}); diff != nil {
		t.Fatal(diff)
	}

	if err := buffer.Put(nil, 4); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond * 50)
	select {
	case n := <-out:
		if diff := deep.Equal(n, FixedBufferCleanerNotification{
			Max:     3,
			Target:  1,
			Size:    4,
			Offsets: []int{0},
			Trim:    3,
		}); diff != nil {
			t.Fatal(diff)
		}
	default:
		t.Fatal("expected")
	}
	time.Sleep(time.Millisecond * 30)

	if diff := deep.Equal(buffer.Slice(), []interface{}{4}); diff != nil {
		t.Fatal(diff)
	}

	v, err := consumer.Get(nil)
	if v != nil {
		t.Fatal("expected nil")
	}
	if err == nil || err.Error() != "bigbuff.Buffer.get offset 1 is 3 past" {
		t.Fatal("unexpected error", err.Error())
	}
}

func TestFixedBufferCleaner_nil(t *testing.T) {
	buffer := new(Buffer)
	defer buffer.Close()
	buffer.SetCleanerConfig(CleanerConfig{
		Cooldown: DefaultCleanerCooldown,
		Cleaner: FixedBufferCleaner(
			3,
			1,
			nil,
		),
	})

	consumer, err := buffer.NewConsumer()
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	defer consumer.Rollback()

	if err := buffer.Put(nil, 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond * 30)

	// ensure that it can be consumed normally, and calls default logic
	if v, err := consumer.Get(nil); err != nil || v != 1 {
		t.Fatal("unexpected values", v, err)
	}
	if err := consumer.Commit(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond * 50)
	if buffer.Size() != 0 {
		t.Error("expected no size")
	}
	time.Sleep(time.Millisecond * 30)

	if err := buffer.Put(nil, 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond * 30)

	if err := buffer.Put(nil, 2); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond * 30)

	if err := buffer.Put(nil, 3); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond * 30)
	if diff := deep.Equal(buffer.Slice(), []interface{}{1, 2, 3}); diff != nil {
		t.Fatal(diff)
	}

	if err := buffer.Put(nil, 4); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond * 50)

	if diff := deep.Equal(buffer.Slice(), []interface{}{4}); diff != nil {
		t.Fatal(diff)
	}

	v, err := consumer.Get(nil)
	if v != nil {
		t.Fatal("expected nil")
	}
	if err == nil || err.Error() != "bigbuff.Buffer.get offset 1 is 3 past" {
		t.Fatal("unexpected error", err.Error())
	}
}

func TestBuffer_Range_empty(t *testing.T) {
	buffer := new(Buffer)
	defer buffer.Close()
	c, err := buffer.NewConsumer()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	err = buffer.Range(nil, c, func(index int, value interface{}) bool {
		t.Error("should not have been reached")
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBuffer_Range_one(t *testing.T) {
	buffer := new(Buffer)
	defer buffer.Close()
	c, err := buffer.NewConsumer()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	err = buffer.Put(nil, -121)
	if err != nil {
		t.Fatal(err)
	}
	var calls int
	err = buffer.Range(nil, c, func(index int, value interface{}) bool {
		if index != 0 || value != -121 {
			t.Error("unexpected range", index, value)
		}
		calls++
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("unexpected number of calls", calls)
	}
}

func TestFatalError_Error(t *testing.T) {
	if v := FatalError(errors.New("some_error")).Error(); v != "some_error" {
		t.Fatal(v)
	}
}

func TestFatalError_panic(t *testing.T) {
	defer func() {
		if v := fmt.Sprint(recover()); v != "bigbuff.FatalError nil err" {
			t.Error(v)
		}
	}()
	_ = FatalError(nil)
}

func TestMinDuration_panicZero(t *testing.T) {
	defer func() {
		if r := recover(); fmt.Sprint(r) != `bigbuff.MinDuration invalid duration: 0s` {
			t.Error(r)
		}
	}()
	MinDuration(0, func() (i interface{}, e error) { return })
	t.Error(`expected panic`)
}

func TestMinDuration_panicNil(t *testing.T) {
	defer func() {
		if r := recover(); fmt.Sprint(r) != `bigbuff.MinDuration nil function` {
			t.Error(r)
		}
	}()
	MinDuration(time.Millisecond, nil)
	t.Error(`expected panic`)
}

func TestMinDuration(t *testing.T) {
	var (
		r  = func() (interface{}, error) { return nil, nil }
		fn = func() func() (time.Duration, interface{}, error) {
			fn := MinDuration(
				time.Millisecond*100,
				func() (interface{}, error) {
					return r()
				},
			)
			return func() (d time.Duration, v interface{}, e error) {
				t := time.Now()
				v, e = fn()
				d = time.Now().Sub(t)
				return
			}
		}()
		diff = func(a, b time.Duration) (c time.Duration) {
			c = a - b
			if c < 0 {
				c *= -1
			}
			return
		}
	)
	if d, v, e := fn(); diff(d, time.Millisecond*100) > time.Millisecond*50 || v != nil || e != nil {
		t.Error(d, v, e)
	}
	r = func() (interface{}, error) { time.Sleep(time.Millisecond * 200); return 512, errors.New(`some_error`) }
	if d, v, e := fn(); diff(d, time.Millisecond*200) > time.Millisecond*50 || v != 512 || e == nil || e.Error() != `some_error` {
		t.Error(d, v, e)
	}
}

func TestRange_nilConsumer(t *testing.T) {
	if err := Range(context.Background(), nil, func(index int, value interface{}) bool { panic(`unexpected`) }); err == nil {
		t.Error(err)
	}
}

func TestRange_nilFn(t *testing.T) {
	if err := Range(context.Background(), struct{ Consumer }{}, nil); err == nil {
		t.Error(err)
	}
}

func TestRange_contextGuard(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Range(ctx, struct{ Consumer }{}, func(index int, value interface{}) bool { panic(`unexpected`) }); err != context.Canceled {
		t.Error(err)
	}
}

func TestRange_getPanic(t *testing.T) {
	ctx := context.WithValue(context.Background(), 1, 2)
	err := errors.New(`some_error`)
	var rollback int
	defer func() {
		if r := recover(); r != err {
			t.Error(r, err)
		}
		if rollback != 1 {
			t.Error(rollback)
		}
	}()
	_ = Range(ctx, mockConsumer{
		get: func(c context.Context) (interface{}, error) {
			if c != ctx {
				t.Error(c, ctx)
			}
			panic(err)
		},
		rollback: func() error {
			rollback++
			return nil
		},
	}, func(index int, value interface{}) bool { panic(`unexpected`) })
}

func TestRange_getNilContext(t *testing.T) {
	err := errors.New(`some_error`)
	var rollback int
	defer func() {
		if r := recover(); r != err {
			t.Error(r, err)
		}
		if rollback != 1 {
			t.Error(rollback)
		}
	}()
	_ = Range(nil, mockConsumer{
		get: func(c context.Context) (interface{}, error) {
			if c != nil {
				t.Error(c)
			}
			panic(err)
		},
		rollback: func() error {
			rollback++
			return nil
		},
	}, func(index int, value interface{}) bool { panic(`unexpected`) })
}

func TestRange_rollback(t *testing.T) {
	var (
		countGet      int
		countFn       int
		countCommit   int
		countRollback int
		last          string
		expected      = errors.New(`some_error`)
		ctx           = context.WithValue(context.Background(), `sasd`, expected)
		actual        = Range(
			ctx,
			mockConsumer{
				get: func(c context.Context) (interface{}, error) {
					if last != `` {
						t.Error(last)
					}
					last = `get`
					countGet++
					if ctx != c {
						t.Error(ctx, c)
					}
					return expected, nil
				},
				rollback: func() error {
					if last != `commit` {
						t.Error(last)
					}
					last = `rollback`
					countRollback++
					return errors.New(`some_other_error`)
				},
				commit: func() error {
					if last != `fn` {
						t.Error(last)
					}
					last = `commit`
					countCommit++
					return expected
				},
			},
			func(index int, value interface{}) bool {
				if last != `get` {
					t.Error(last)
				}
				last = `fn`
				countFn++
				if index != 0 {
					t.Error(index)
				}
				if value != expected {
					t.Error(value)
				}
				return false
			},
		)
	)
	if expected != actual {
		t.Error(actual)
	}
	if last != `rollback` {
		t.Error(last)
	}
	if countGet != 1 {
		t.Error(countGet)
	}
	if countFn != 1 {
		t.Error(countFn)
	}
	if countCommit != 1 {
		t.Error(countCommit)
	}
	if countRollback != 1 {
		t.Error(countRollback)
	}
}

type mockConsumer struct {
	Consumer
	get      func(ctx context.Context) (interface{}, error)
	commit   func() error
	rollback func() error
}

func (m mockConsumer) Get(ctx context.Context) (interface{}, error) {
	if m.get != nil {
		return m.get(ctx)
	}
	return m.Consumer.Get(ctx)
}

func (m mockConsumer) Commit() error {
	if m.commit != nil {
		return m.commit()
	}
	return m.Consumer.Commit()
}

func (m mockConsumer) Rollback() error {
	if m.rollback != nil {
		return m.rollback()
	}
	return m.Consumer.Rollback()
}

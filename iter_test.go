package bigbuff

import (
	"context"
	"errors"
	"testing"
)

// mockGetter implements Getter[T] for testing
type mockGetter[T any] struct {
	values   []T
	errors   []error
	callIdx  int
	getCalls int
}

func newMockGetter[T any](values []T, errors []error) *mockGetter[T] {
	return &mockGetter[T]{
		values: values,
		errors: errors,
	}
}

func (m *mockGetter[T]) Get(ctx context.Context) (T, error) {
	m.getCalls++

	// Check context cancellation first
	if err := ctx.Err(); err != nil {
		var zero T
		return zero, err
	}

	if m.callIdx >= len(m.values) && m.callIdx >= len(m.errors) {
		// If we've exhausted both values and errors, block until context cancellation
		<-ctx.Done()
		var zero T
		return zero, ctx.Err()
	}

	var err error
	if m.callIdx < len(m.errors) {
		err = m.errors[m.callIdx]
	}

	var value T
	if m.callIdx < len(m.values) {
		value = m.values[m.callIdx]
	}

	m.callIdx++
	return value, err
}

func TestSeq_NilContext(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for nil context")
		} else if r != "bigbuff.Seq requires non-nil ctx" {
			t.Errorf("Expected specific panic message, got: %v", r)
		}
	}()

	source := newMockGetter([]int{1}, nil)
	Seq[int](nil, source)
}

func TestSeq_NilSource(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for nil source")
		} else if r != "bigbuff.Seq requires non-nil source" {
			t.Errorf("Expected specific panic message, got: %v", r)
		}
	}()

	ctx := context.Background()
	Seq[int](ctx, nil)
}

func TestSeq_PreCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	source := newMockGetter([]int{1, 2, 3}, nil)
	seq := Seq(ctx, source)

	var values []int
	var errors []error

	seq(func(value int, err error) bool {
		values = append(values, value)
		errors = append(errors, err)
		return true
	})

	// Should get one yield with zero value and context error
	if len(values) != 1 {
		t.Errorf("Expected 1 yield, got %d", len(values))
	}
	if values[0] != 0 {
		t.Errorf("Expected zero value, got %d", values[0])
	}
	if len(errors) != 1 || errors[0] != context.Canceled {
		t.Errorf("Expected context.Canceled error, got %v", errors)
	}

	// Source.Get should not have been called
	if source.getCalls != 0 {
		t.Errorf("Expected 0 Get calls, got %d", source.getCalls)
	}
}

func TestSeq_SingleValue(t *testing.T) {
	ctx := context.Background()
	source := newMockGetter([]int{42}, []error{nil, errors.New("end")})
	seq := Seq(ctx, source)

	var values []int
	var errors []error

	seq(func(value int, err error) bool {
		values = append(values, value)
		errors = append(errors, err)
		return true
	})

	// Should get: (42, nil), (0, "end")
	expectedValues := []int{42, 0}
	expectedErrorCount := 2

	if len(values) != len(expectedValues) {
		t.Errorf("Expected %d values, got %d: %v", len(expectedValues), len(values), values)
	}

	for i, expected := range expectedValues {
		if i < len(values) && values[i] != expected {
			t.Errorf("values[%d]: expected %d, got %d", i, expected, values[i])
		}
	}

	if len(errors) != expectedErrorCount {
		t.Errorf("Expected %d errors, got %d: %v", expectedErrorCount, len(errors), errors)
	}

	// First error should be nil, second should be the "end" error
	if errors[0] != nil {
		t.Errorf("Expected first error to be nil, got %v", errors[0])
	}
	if errors[1] == nil || errors[1].Error() != "end" {
		t.Errorf("Expected second error to be 'end', got %v", errors[1])
	}

	if source.getCalls != 2 {
		t.Errorf("Expected 2 Get calls, got %d", source.getCalls)
	}
}

func TestSeq_MultipleValues(t *testing.T) {
	ctx := context.Background()
	source := newMockGetter([]int{1, 2, 3}, []error{nil, nil, nil, errors.New("done")})
	seq := Seq(ctx, source)

	var values []int
	var errors []error

	seq(func(value int, err error) bool {
		values = append(values, value)
		errors = append(errors, err)
		return true
	})

	// Should get: (1, nil), (2, nil), (3, nil), (0, "done")
	expectedValues := []int{1, 2, 3, 0}

	if len(values) != len(expectedValues) {
		t.Errorf("Expected %d values, got %d: %v", len(expectedValues), len(values), values)
	}

	for i, expected := range expectedValues {
		if i < len(values) && values[i] != expected {
			t.Errorf("values[%d]: expected %d, got %d", i, expected, values[i])
		}
	}

	// First 3 errors should be nil, last should be "done"
	for i := 0; i < 3; i++ {
		if i < len(errors) && errors[i] != nil {
			t.Errorf("errors[%d]: expected nil, got %v", i, errors[i])
		}
	}
	if len(errors) < 4 || errors[3] == nil || errors[3].Error() != "done" {
		t.Errorf("Expected final error to be 'done', got %v", errors)
	}

	if source.getCalls != 4 {
		t.Errorf("Expected 4 Get calls, got %d", source.getCalls)
	}
}

func TestSeq_EarlyBreak(t *testing.T) {
	ctx := context.Background()
	source := newMockGetter([]int{1, 2, 3, 4, 5}, []error{nil, nil, nil, nil, nil, errors.New("end")})
	seq := Seq(ctx, source)

	var values []int
	var errors []error
	callCount := 0

	seq(func(value int, err error) bool {
		values = append(values, value)
		errors = append(errors, err)
		callCount++
		return callCount < 2 // Break after 2 calls
	})

	// Should get: (1, nil), (2, nil) then stop
	expectedValues := []int{1, 2}

	if len(values) != len(expectedValues) {
		t.Errorf("Expected %d values, got %d: %v", len(expectedValues), len(values), values)
	}

	for i, expected := range expectedValues {
		if i < len(values) && values[i] != expected {
			t.Errorf("values[%d]: expected %d, got %d", i, expected, values[i])
		}
	}

	// Both errors should be nil
	for i, err := range errors {
		if err != nil {
			t.Errorf("errors[%d]: expected nil, got %v", i, err)
		}
	}

	if source.getCalls != 2 {
		t.Errorf("Expected 2 Get calls, got %d", source.getCalls)
	}
}

func TestSeq_ImmediateError(t *testing.T) {
	ctx := context.Background()
	testErr := errors.New("immediate error")
	source := newMockGetter([]int{}, []error{testErr})
	seq := Seq(ctx, source)

	var values []int
	var errors []error

	seq(func(value int, err error) bool {
		values = append(values, value)
		errors = append(errors, err)
		return true
	})

	// Should get: (0, testErr)
	if len(values) != 1 || values[0] != 0 {
		t.Errorf("Expected [0], got %v", values)
	}

	if len(errors) != 1 || errors[0] != testErr {
		t.Errorf("Expected [%v], got %v", testErr, errors)
	}

	if source.getCalls != 1 {
		t.Errorf("Expected 1 Get call, got %d", source.getCalls)
	}
}

func TestSeq_ErrorAfterValues(t *testing.T) {
	ctx := context.Background()
	testErr := errors.New("error after values")
	source := newMockGetter([]int{10, 20}, []error{nil, nil, testErr})
	seq := Seq(ctx, source)

	var values []int
	var errors []error

	seq(func(value int, err error) bool {
		values = append(values, value)
		errors = append(errors, err)
		return true
	})

	// Should get: (10, nil), (20, nil), (0, testErr)
	expectedValues := []int{10, 20, 0}

	if len(values) != len(expectedValues) {
		t.Errorf("Expected %v, got %v", expectedValues, values)
	}

	for i, expected := range expectedValues {
		if i < len(values) && values[i] != expected {
			t.Errorf("values[%d]: expected %d, got %d", i, expected, values[i])
		}
	}

	// First two errors should be nil, third should be testErr
	if len(errors) != 3 {
		t.Errorf("Expected 3 errors, got %d: %v", len(errors), errors)
	}
	if errors[0] != nil || errors[1] != nil {
		t.Errorf("Expected first two errors to be nil, got %v", errors[:2])
	}
	if errors[2] != testErr {
		t.Errorf("Expected third error to be %v, got %v", testErr, errors[2])
	}

	if source.getCalls != 3 {
		t.Errorf("Expected 3 Get calls, got %d", source.getCalls)
	}
}

func TestSeq_ContextCancellationDuringIteration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create a mock that will return one value, then block until context cancellation
	source := newMockGetter([]int{100}, nil) // Only one value, no errors
	seq := Seq(ctx, source)

	var values []int
	var errors []error
	callCount := 0

	seq(func(value int, err error) bool {
		values = append(values, value)
		errors = append(errors, err)
		callCount++

		// Cancel context after first successful value
		if callCount == 1 && err == nil {
			cancel()
		}

		return true
	})

	// Should get: (100, nil), then context cancellation on next Get call: (0, Canceled)
	expectedValues := []int{100, 0}

	if len(values) != len(expectedValues) {
		t.Errorf("Expected %v, got %v", expectedValues, values)
	}

	if len(errors) != 2 {
		t.Errorf("Expected 2 errors, got %d: %v", len(errors), errors)
	}

	if errors[0] != nil {
		t.Errorf("Expected first error to be nil, got %v", errors[0])
	}

	if errors[1] != context.Canceled {
		t.Errorf("Expected second error to be context.Canceled, got %v", errors[1])
	}

	if source.getCalls != 2 {
		t.Errorf("Expected 2 Get calls, got %d", source.getCalls)
	}
}

func TestSeq_DifferentTypes(t *testing.T) {
	// Test with string type
	ctx := context.Background()
	source := newMockGetter([]string{"hello", "world"}, []error{nil, nil, errors.New("done")})
	seq := Seq(ctx, source)

	var values []string
	var errors []error

	seq(func(value string, err error) bool {
		values = append(values, value)
		errors = append(errors, err)
		return true
	})

	expectedValues := []string{"hello", "world", ""}

	if len(values) != len(expectedValues) {
		t.Errorf("Expected %v, got %v", expectedValues, values)
	}

	for i, expected := range expectedValues {
		if i < len(values) && values[i] != expected {
			t.Errorf("values[%d]: expected %q, got %q", i, expected, values[i])
		}
	}
}

// Test to ensure coverage of the zero value creation line
func TestSeq_ZeroValueHandling(t *testing.T) {
	ctx := context.Background()
	testErr := errors.New("test error")

	// Test with a struct type to ensure zero value handling works for complex types
	type testStruct struct {
		ID   int
		Name string
	}

	source := newMockGetter([]testStruct{}, []error{testErr})
	seq := Seq(ctx, source)

	var values []testStruct
	var errors []error

	seq(func(value testStruct, err error) bool {
		values = append(values, value)
		errors = append(errors, err)
		return true
	})

	// Should get zero value struct
	if len(values) != 1 {
		t.Errorf("Expected 1 value, got %d", len(values))
	}

	expected := testStruct{} // Zero value
	if values[0] != expected {
		t.Errorf("Expected zero value %+v, got %+v", expected, values[0])
	}

	if len(errors) != 1 || errors[0] != testErr {
		t.Errorf("Expected error %v, got %v", testErr, errors)
	}
}

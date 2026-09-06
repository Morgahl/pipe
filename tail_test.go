package pipe_test

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Morgahl/pipe"
)

var errBoom = errors.New("boom")

func TestTailPull(t *testing.T) {
	t.Run("value", func(t *testing.T) {
		head, tail := pipe.New[int](1)
		head.Push(7)
		require.Equal(t, 7, tail.Pull())
	})

	t.Run("closedEmpty", func(t *testing.T) {
		head, tail := pipe.New[int](1)
		head.Close()
		require.Equal(t, 0, tail.Pull())
	})

	t.Run("blocksUntilValue", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			require := require.New(t)
			head, tail := pipe.New[int](1)
			var got int
			done := false
			go func() {
				got = tail.Pull()
				done = true
			}()
			synctest.Wait()
			require.False(done, "Pull returned with no value")
			head.Push(7)
			synctest.Wait()
			require.True(done, "Pull blocked after a value arrived")
			require.Equal(7, got)
		})
	})
}

func TestTailPullSafe(t *testing.T) {
	t.Run("value", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Push(7)
		got, ok := tail.PullSafe()
		require.True(ok)
		require.Equal(7, got)
	})

	t.Run("closedEmpty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		got, ok := tail.PullSafe()
		require.False(ok)
		require.Equal(0, got)
	})

	t.Run("blocksUntilValue", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			require := require.New(t)
			head, tail := pipe.New[int](1)
			var got int
			var ok, done bool
			go func() {
				got, ok = tail.PullSafe()
				done = true
			}()
			synctest.Wait()
			require.False(done, "PullSafe returned with no value")
			head.Push(7)
			synctest.Wait()
			require.True(done, "PullSafe blocked after a value arrived")
			require.True(ok)
			require.Equal(7, got)
		})
	})
}

func TestTailTryPull(t *testing.T) {
	t.Run("value", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Push(7)
		got, ok := tail.TryPull()
		require.True(ok)
		require.Equal(7, got)
	})

	t.Run("openEmpty", func(t *testing.T) {
		_, tail := pipe.New[int](1)
		_, ok := tail.TryPull()
		require.False(t, ok, "TryPull on open empty channel")
	})

	t.Run("closed", func(t *testing.T) {
		head, tail := pipe.New[int](1)
		head.Close()
		_, ok := tail.TryPull()
		require.False(t, ok, "TryPull on closed channel")
	})

	t.Run("nil", func(t *testing.T) {
		var tail pipe.Tail[int]
		_, ok := tail.TryPull()
		require.False(t, ok, "TryPull on nil channel")
	})
}

func TestTailDrain(t *testing.T) {
	t.Run("values", func(t *testing.T) {
		head, tail := pipe.New[int](3)
		head.Push(1)
		head.Push(2)
		head.Push(3)
		head.Close()
		tail.Drain()
		_, ok := tail.PullSafe()
		require.False(t, ok, "value left after Drain")
	})

	t.Run("closedEmpty", func(t *testing.T) {
		head, tail := pipe.New[int](1)
		head.Close()
		tail.Drain()
	})
}

func TestTailWait(t *testing.T) {
	t.Run("consumesOne", func(t *testing.T) {
		head, tail := pipe.New[int](2)
		head.Push(1)
		head.Push(2)
		tail.Wait()
		require.Equal(t, 2, tail.Pull())
	})

	t.Run("closedEmpty", func(t *testing.T) {
		head, tail := pipe.New[int](1)
		head.Close()
		tail.Wait()
	})

	t.Run("blocksUntilValue", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			require := require.New(t)
			head, tail := pipe.New[int](1)
			done := false
			go func() {
				tail.Wait()
				done = true
			}()
			synctest.Wait()
			require.False(done, "Wait returned with no value")
			head.Push(7)
			synctest.Wait()
			require.True(done, "Wait blocked after a value arrived")
		})
	})
}

func TestFanIn(t *testing.T) {
	t.Run("none", func(t *testing.T) {
		require := require.New(t)
		out := pipe.FanIn[int](1)
		require.Equal(1, cap(out))
		_, ok := out.PullSafe()
		require.False(ok, "FanIn of no tails returned a value")
	})

	t.Run("emptyInput", func(t *testing.T) {
		head, tail := pipe.New[int](1)
		head.Close()
		out := pipe.FanIn(1, tail)
		_, ok := out.PullSafe()
		require.False(t, ok, "got a value, want closed")
	})

	t.Run("one", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](3)
		head.Push(1)
		head.Push(2)
		head.Push(3)
		head.Close()
		out := pipe.FanIn(3, tail)
		require.Equal(3, cap(out))
		var got []int
		for v := range out {
			got = append(got, v)
		}
		require.Equal([]int{1, 2, 3}, got)
	})

	t.Run("three", func(t *testing.T) {
		require := require.New(t)
		var tails []pipe.Tail[int]
		for i := range 3 {
			head, tail := pipe.New[int](2)
			head.Push(i*2 + 1)
			head.Push(i*2 + 2)
			head.Close()
			tails = append(tails, tail)
		}
		out := pipe.FanIn(6, tails...)
		require.Equal(6, cap(out))
		var got []int
		for v := range out {
			got = append(got, v)
		}
		slices.Sort(got)
		require.Equal([]int{1, 2, 3, 4, 5, 6}, got)
	})

	t.Run("closesAfterLastInput", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			require := require.New(t)
			doneHead, doneTail := pipe.New[int](1)
			doneHead.Push(1)
			doneHead.Close()
			openHead, openTail := pipe.New[int](1)
			out := pipe.FanIn(1, doneTail, openTail)
			require.Equal(1, out.Pull())
			var closed bool
			go func() {
				_, ok := out.PullSafe()
				closed = !ok
			}()
			synctest.Wait()
			require.False(closed, "FanIn closed while an input was open")
			openHead.Close()
			synctest.Wait()
			require.True(closed, "FanIn open after last input closed")
		})
	})
}

func TestSource(t *testing.T) {
	for _, repeat := range []int{0, -2} {
		t.Run(fmt.Sprintf("repeat%d", repeat), func(t *testing.T) {
			require := require.New(t)
			calls, closes := 0, 0
			out := pipe.Source(repeat, 1, func() int { calls++; return calls }, func() { closes++ })
			require.Equal(1, cap(out))
			_, ok := out.PullSafe()
			require.False(ok, "got a value, want closed")
			require.Equal(0, calls, "source calls")
			require.Equal(1, closes, "closer calls")
		})
	}

	t.Run("repeat3", func(t *testing.T) {
		require := require.New(t)
		calls, closes := 0, 0
		out := pipe.Source(3, 2, func() int { calls++; return calls }, func() { closes++ })
		require.Equal(2, cap(out))
		var got []int
		for v := range out {
			got = append(got, v)
		}
		require.Equal([]int{1, 2, 3}, got)
		require.Equal(1, closes, "closer calls")
	})

	t.Run("forever", func(t *testing.T) {
		calls := 0
		out := pipe.Source(pipe.RepeatForever, 1, func() int { calls++; return calls }, func() {})
		var got []int
		for range 5 {
			got = append(got, out.Pull())
		}
		require.Equal(t, []int{1, 2, 3, 4, 5}, got)
	})
}

func TestSourceError(t *testing.T) {
	t.Run("repeat0", func(t *testing.T) {
		require := require.New(t)
		calls, closes := 0, 0
		out, errs := pipe.SourceError(0, 1, func() (int, error) { calls++; return calls, nil }, func() { closes++ })
		_, ok := out.PullSafe()
		require.False(ok, "got a value, want closed")
		_, ok = errs.PullSafe()
		require.False(ok, "got an error, want closed")
		require.Equal(0, calls, "source calls")
		require.Equal(1, closes, "closer calls")
	})

	t.Run("split", func(t *testing.T) {
		require := require.New(t)
		calls, closes := 0, 0
		out, errs := pipe.SourceError(4, 4, func() (int, error) {
			calls++
			if calls%2 == 0 {
				return 0, errBoom
			}
			return calls, nil
		}, func() { closes++ })
		require.Equal(4, cap(out))
		require.Equal(4, cap(errs))
		var got []int
		for v := range out {
			got = append(got, v)
		}
		var gotErrs []error
		for err := range errs {
			gotErrs = append(gotErrs, err)
		}
		require.Equal([]int{1, 3}, got)
		require.Equal([]error{errBoom, errBoom}, gotErrs)
		require.Equal(1, closes, "closer calls")
	})

	t.Run("done", func(t *testing.T) {
		require := require.New(t)
		calls, closes := 0, 0
		out, errs := pipe.SourceError(10, 10, func() (int, error) {
			calls++
			if calls == 3 {
				return 0, pipe.Done
			}
			return calls, nil
		}, func() { closes++ })
		var got []int
		for v := range out {
			got = append(got, v)
		}
		require.Equal([]int{1, 2}, got)
		err, ok := errs.PullSafe()
		require.False(ok, "Done pushed as error %v", err)
		require.Equal(3, calls, "source calls")
		require.Equal(1, closes, "closer calls")
	})

	t.Run("wrappedDone", func(t *testing.T) {
		require := require.New(t)
		calls := 0
		out, errs := pipe.SourceError(10, 10, func() (int, error) {
			calls++
			if calls == 3 {
				return 0, fmt.Errorf("wrap: %w", pipe.Done)
			}
			return calls, nil
		}, func() {})
		var got []int
		for v := range out {
			got = append(got, v)
		}
		require.Equal([]int{1, 2}, got)
		err, ok := errs.PullSafe()
		require.False(ok, "wrapped Done pushed as error %v", err)
	})

	t.Run("exhausted", func(t *testing.T) {
		require := require.New(t)
		calls := 0
		out, errs := pipe.SourceError(2, 2, func() (int, error) { calls++; return calls, nil }, func() {})
		var got []int
		for v := range out {
			got = append(got, v)
		}
		require.Equal([]int{1, 2}, got)
		_, ok := errs.PullSafe()
		require.False(ok, "error tail open after exhaustion")
	})

	t.Run("forever", func(t *testing.T) {
		require := require.New(t)
		calls := 0
		out, errs := pipe.SourceError(pipe.RepeatForever, 1, func() (int, error) {
			calls++
			if calls%2 == 0 {
				return 0, errBoom
			}
			return calls, nil
		}, func() {})
		var got []int
		var gotErrs []error
		for range 3 {
			got = append(got, out.Pull())
			gotErrs = append(gotErrs, errs.Pull())
		}
		require.Equal([]int{1, 3, 5}, got)
		require.Equal([]error{errBoom, errBoom, errBoom}, gotErrs)
	})

	t.Run("foreverDone", func(t *testing.T) {
		require := require.New(t)
		calls, closes := 0, 0
		out, errs := pipe.SourceError(pipe.RepeatForever, 10, func() (int, error) {
			calls++
			if calls == 3 {
				return 0, pipe.Done
			}
			return calls, nil
		}, func() { closes++ })
		var got []int
		for v := range out {
			got = append(got, v)
		}
		require.Equal([]int{1, 2}, got)
		err, ok := errs.PullSafe()
		require.False(ok, "Done pushed as error %v", err)
		require.Equal(3, calls, "source calls")
		require.Equal(1, closes, "closer calls")
	})
}

func TestSourceErrorSink(t *testing.T) {
	t.Run("repeat0", func(t *testing.T) {
		require := require.New(t)
		calls, closes := 0, 0
		var sunk []error
		out := pipe.SourceErrorSink(0, 1, func() (int, error) { calls++; return calls, nil }, func() { closes++ }, func(err error) { sunk = append(sunk, err) })
		_, ok := out.PullSafe()
		require.False(ok, "got a value, want closed")
		require.Empty(sunk)
		require.Equal(0, calls, "source calls")
		require.Equal(1, closes, "closer calls")
	})

	t.Run("sunk", func(t *testing.T) {
		require := require.New(t)
		calls, closes := 0, 0
		var sunk []error
		out := pipe.SourceErrorSink(4, 4, func() (int, error) {
			calls++
			if calls%2 == 0 {
				return 0, errBoom
			}
			return calls, nil
		}, func() { closes++ }, func(err error) { sunk = append(sunk, err) })
		require.Equal(4, cap(out))
		var got []int
		for v := range out {
			got = append(got, v)
		}
		require.Equal([]int{1, 3}, got)
		require.Equal([]error{errBoom, errBoom}, sunk)
		require.Equal(1, closes, "closer calls")
	})

	t.Run("doneNotSunk", func(t *testing.T) {
		require := require.New(t)
		calls, closes := 0, 0
		var sunk []error
		out := pipe.SourceErrorSink(10, 10, func() (int, error) {
			calls++
			if calls == 3 {
				return 0, pipe.Done
			}
			return calls, nil
		}, func() { closes++ }, func(err error) { sunk = append(sunk, err) })
		var got []int
		for v := range out {
			got = append(got, v)
		}
		require.Equal([]int{1, 2}, got)
		require.Empty(sunk)
		require.Equal(1, closes, "closer calls")
	})

	t.Run("forever", func(t *testing.T) {
		require := require.New(t)
		calls := 0
		sunk := make(chan error, 3)
		out := pipe.SourceErrorSink(pipe.RepeatForever, 1, func() (int, error) {
			calls++
			if calls%2 == 0 {
				return 0, errBoom
			}
			return calls, nil
		}, func() {}, func(err error) { sunk <- err })
		var got []int
		var gotErrs []error
		for range 3 {
			got = append(got, out.Pull())
			gotErrs = append(gotErrs, <-sunk)
		}
		require.Equal([]int{1, 3, 5}, got)
		require.Equal([]error{errBoom, errBoom, errBoom}, gotErrs)
	})

	t.Run("foreverDone", func(t *testing.T) {
		require := require.New(t)
		calls, closes := 0, 0
		var sunk []error
		out := pipe.SourceErrorSink(pipe.RepeatForever, 10, func() (int, error) {
			calls++
			if calls == 3 {
				return 0, pipe.Done
			}
			return calls, nil
		}, func() { closes++ }, func(err error) { sunk = append(sunk, err) })
		var got []int
		for v := range out {
			got = append(got, v)
		}
		require.Equal([]int{1, 2}, got)
		require.Empty(sunk)
		require.Equal(3, calls, "source calls")
		require.Equal(1, closes, "closer calls")
	})
}

func TestTailFanOut(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			require := require.New(t)
			head, tail := pipe.New[int](3)
			head.Push(1)
			head.Push(2)
			head.Push(3)
			head.Close()
			tails := tail.FanOut(0)
			require.Empty(tails)
			synctest.Wait()
			_, ok := tail.TryPull()
			require.False(ok, "input not drained")
		})
	})

	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		tails := tail.FanOut(2)
		require.Len(tails, 2)
		for i, tl := range tails {
			_, ok := tl.PullSafe()
			require.False(ok, "tail %d got a value, want closed", i)
		}
	})

	t.Run("three", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](3)
		head.Push(1)
		head.Push(2)
		head.Push(3)
		head.Close()
		tails := tail.FanOut(3)
		require.Len(tails, 3)
		for i, tl := range tails {
			require.Equal(3, cap(tl), "tail %d cap", i)
			var got []int
			for v := range tl {
				got = append(got, v)
			}
			require.Equal([]int{1, 2, 3}, got, "tail %d", i)
		}
	})
}

func TestTailFilter(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		out := tail.Filter(func(i int) bool { return i%2 == 0 })
		require.Equal(1, cap(out))
		_, ok := out.PullSafe()
		require.False(ok, "got a value, want closed")
	})

	t.Run("mixed", func(t *testing.T) {
		head, tail := pipe.New[int](6)
		for i := 1; i <= 6; i++ {
			head.Push(i)
		}
		head.Close()
		out := tail.Filter(func(i int) bool { return i%2 == 0 })
		var got []int
		for v := range out {
			got = append(got, v)
		}
		require.Equal(t, []int{2, 4, 6}, got)
	})

	t.Run("dropAll", func(t *testing.T) {
		head, tail := pipe.New[int](3)
		head.Push(1)
		head.Push(2)
		head.Push(3)
		head.Close()
		out := tail.Filter(func(int) bool { return false })
		v, ok := out.PullSafe()
		require.False(t, ok, "got %d, want closed", v)
	})
}

func TestTailFilterAsync(t *testing.T) {
	for _, workers := range []int{0, 4} {
		t.Run(fmt.Sprintf("workers%d", workers), func(t *testing.T) {
			require := require.New(t)
			head, tail := pipe.New[int](6)
			for i := 1; i <= 6; i++ {
				head.Push(i)
			}
			head.Close()
			out := tail.FilterAsync(workers, func(i int) bool { return i%2 == 0 })
			require.Equal(6, cap(out))
			var got []int
			for v := range out {
				got = append(got, v)
			}
			slices.Sort(got)
			require.Equal([]int{2, 4, 6}, got)
		})
	}

	t.Run("empty", func(t *testing.T) {
		head, tail := pipe.New[int](1)
		head.Close()
		out := tail.FilterAsync(4, func(int) bool { return true })
		_, ok := out.PullSafe()
		require.False(t, ok, "got a value, want closed")
	})
}

// keepOddErrOnFour keeps odd values, drops 2, errors on 4.
func keepOddErrOnFour(i int) (bool, error) {
	switch i {
	case 4:
		return false, errBoom
	case 2:
		return false, nil
	}
	return true, nil
}

func TestTailFilterError(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		out, errs := tail.FilterError(keepOddErrOnFour)
		_, ok := out.PullSafe()
		require.False(ok, "got a value, want closed")
		_, ok = errs.PullSafe()
		require.False(ok, "got an error, want closed")
	})

	t.Run("split", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](4)
		for i := 1; i <= 4; i++ {
			head.Push(i)
		}
		head.Close()
		out, errs := tail.FilterError(keepOddErrOnFour)
		require.Equal(4, cap(out))
		require.Equal(4, cap(errs))
		var got []int
		for v := range out {
			got = append(got, v)
		}
		var gotErrs []error
		for err := range errs {
			gotErrs = append(gotErrs, err)
		}
		require.Equal([]int{1, 3}, got)
		require.Equal([]error{errBoom}, gotErrs)
	})
}

func TestTailFilterErrorAsync(t *testing.T) {
	for _, workers := range []int{0, 4} {
		t.Run(fmt.Sprintf("workers%d", workers), func(t *testing.T) {
			require := require.New(t)
			head, tail := pipe.New[int](4)
			for i := 1; i <= 4; i++ {
				head.Push(i)
			}
			head.Close()
			out, errs := tail.FilterErrorAsync(workers, keepOddErrOnFour)
			require.Equal(4, cap(out))
			require.Equal(4, cap(errs))
			var got []int
			for v := range out {
				got = append(got, v)
			}
			var gotErrs []error
			for err := range errs {
				gotErrs = append(gotErrs, err)
			}
			slices.Sort(got)
			require.Equal([]int{1, 3}, got)
			require.Equal([]error{errBoom}, gotErrs)
		})
	}

	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		out, errs := tail.FilterErrorAsync(4, keepOddErrOnFour)
		_, ok := out.PullSafe()
		require.False(ok, "got a value, want closed")
		_, ok = errs.PullSafe()
		require.False(ok, "got an error, want closed")
	})
}

func TestTailFilterErrorSink(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		var sunk []error
		out := tail.FilterErrorSink(keepOddErrOnFour, func(err error) { sunk = append(sunk, err) })
		_, ok := out.PullSafe()
		require.False(ok, "got a value, want closed")
		require.Empty(sunk)
	})

	t.Run("split", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](4)
		for i := 1; i <= 4; i++ {
			head.Push(i)
		}
		head.Close()
		var sunk []error
		out := tail.FilterErrorSink(keepOddErrOnFour, func(err error) { sunk = append(sunk, err) })
		require.Equal(4, cap(out))
		var got []int
		for v := range out {
			got = append(got, v)
		}
		require.Equal([]int{1, 3}, got)
		require.Equal([]error{errBoom}, sunk)
	})
}

func TestTailFilterErrorSinkAsync(t *testing.T) {
	for _, workers := range []int{0, 4} {
		t.Run(fmt.Sprintf("workers%d", workers), func(t *testing.T) {
			require := require.New(t)
			head, tail := pipe.New[int](4)
			for i := 1; i <= 4; i++ {
				head.Push(i)
			}
			head.Close()
			sunk := make(chan error, 4)
			out := tail.FilterErrorSinkAsync(workers, keepOddErrOnFour, func(err error) { sunk <- err })
			require.Equal(4, cap(out))
			var got []int
			for v := range out {
				got = append(got, v)
			}
			slices.Sort(got)
			require.Equal([]int{1, 3}, got)
			require.Len(sunk, 1)
			require.Equal(errBoom, <-sunk)
		})
	}

	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		sunk := make(chan error, 1)
		out := tail.FilterErrorSinkAsync(4, keepOddErrOnFour, func(err error) { sunk <- err })
		_, ok := out.PullSafe()
		require.False(ok, "got a value, want closed")
		require.Empty(sunk)
	})
}

func TestTailMap(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](2)
		head.Close()
		out := tail.Map(func(i int) int { return i * 2 })
		require.Equal(2, cap(out))
		_, ok := out.PullSafe()
		require.False(ok, "got a value, want closed")
	})

	t.Run("order", func(t *testing.T) {
		head, tail := pipe.New[int](3)
		head.Push(1)
		head.Push(2)
		head.Push(3)
		head.Close()
		out := tail.Map(func(i int) int { return i * 2 })
		var got []int
		for v := range out {
			got = append(got, v)
		}
		require.Equal(t, []int{2, 4, 6}, got)
	})

	t.Run("typeChange", func(t *testing.T) {
		head, tail := pipe.New[int](3)
		head.Push(1)
		head.Push(2)
		head.Push(3)
		head.Close()
		out := tail.Map(strconv.Itoa)
		var got []string
		for v := range out {
			got = append(got, v)
		}
		require.Equal(t, []string{"1", "2", "3"}, got)
	})
}

func TestTailMapAsync(t *testing.T) {
	for _, workers := range []int{0, 4} {
		t.Run(fmt.Sprintf("workers%d", workers), func(t *testing.T) {
			require := require.New(t)
			head, tail := pipe.New[int](3)
			head.Push(1)
			head.Push(2)
			head.Push(3)
			head.Close()
			out := tail.MapAsync(workers, func(i int) int { return i * 2 })
			require.Equal(3, cap(out))
			var got []int
			for v := range out {
				got = append(got, v)
			}
			slices.Sort(got)
			require.Equal([]int{2, 4, 6}, got)
		})
	}

	t.Run("empty", func(t *testing.T) {
		head, tail := pipe.New[int](1)
		head.Close()
		out := tail.MapAsync(4, func(i int) int { return i })
		_, ok := out.PullSafe()
		require.False(t, ok, "got a value, want closed")
	})
}

// doubleErrOnTwo doubles every value but 2, which errors.
func doubleErrOnTwo(i int) (int, error) {
	if i == 2 {
		return 0, errBoom
	}
	return i * 2, nil
}

func TestTailMapError(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		out, errs := tail.MapError(doubleErrOnTwo)
		_, ok := out.PullSafe()
		require.False(ok, "got a value, want closed")
		_, ok = errs.PullSafe()
		require.False(ok, "got an error, want closed")
	})

	t.Run("split", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](3)
		head.Push(1)
		head.Push(2)
		head.Push(3)
		head.Close()
		out, errs := tail.MapError(doubleErrOnTwo)
		require.Equal(3, cap(out))
		require.Equal(3, cap(errs))
		var got []int
		for v := range out {
			got = append(got, v)
		}
		var gotErrs []error
		for err := range errs {
			gotErrs = append(gotErrs, err)
		}
		require.Equal([]int{2, 6}, got)
		require.Equal([]error{errBoom}, gotErrs)
	})
}

func TestTailMapErrorAsync(t *testing.T) {
	for _, workers := range []int{0, 4} {
		t.Run(fmt.Sprintf("workers%d", workers), func(t *testing.T) {
			require := require.New(t)
			head, tail := pipe.New[int](3)
			head.Push(1)
			head.Push(2)
			head.Push(3)
			head.Close()
			out, errs := tail.MapErrorAsync(workers, doubleErrOnTwo)
			require.Equal(3, cap(out))
			require.Equal(3, cap(errs))
			var got []int
			for v := range out {
				got = append(got, v)
			}
			var gotErrs []error
			for err := range errs {
				gotErrs = append(gotErrs, err)
			}
			slices.Sort(got)
			require.Equal([]int{2, 6}, got)
			require.Equal([]error{errBoom}, gotErrs)
		})
	}

	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		out, errs := tail.MapErrorAsync(4, doubleErrOnTwo)
		_, ok := out.PullSafe()
		require.False(ok, "got a value, want closed")
		_, ok = errs.PullSafe()
		require.False(ok, "got an error, want closed")
	})
}

func TestTailMapErrorSink(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		var sunk []error
		out := tail.MapErrorSink(doubleErrOnTwo, func(err error) { sunk = append(sunk, err) })
		_, ok := out.PullSafe()
		require.False(ok, "got a value, want closed")
		require.Empty(sunk)
	})

	t.Run("split", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](3)
		head.Push(1)
		head.Push(2)
		head.Push(3)
		head.Close()
		var sunk []error
		out := tail.MapErrorSink(doubleErrOnTwo, func(err error) { sunk = append(sunk, err) })
		require.Equal(3, cap(out))
		var got []int
		for v := range out {
			got = append(got, v)
		}
		require.Equal([]int{2, 6}, got)
		require.Equal([]error{errBoom}, sunk)
	})
}

func TestTailMapErrorSinkAsync(t *testing.T) {
	for _, workers := range []int{0, 4} {
		t.Run(fmt.Sprintf("workers%d", workers), func(t *testing.T) {
			require := require.New(t)
			head, tail := pipe.New[int](3)
			head.Push(1)
			head.Push(2)
			head.Push(3)
			head.Close()
			sunk := make(chan error, 3)
			out := tail.MapErrorSinkAsync(workers, doubleErrOnTwo, func(err error) { sunk <- err })
			require.Equal(3, cap(out))
			var got []int
			for v := range out {
				got = append(got, v)
			}
			slices.Sort(got)
			require.Equal([]int{2, 6}, got)
			require.Len(sunk, 1)
			require.Equal(errBoom, <-sunk)
		})
	}

	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		sunk := make(chan error, 1)
		out := tail.MapErrorSinkAsync(4, doubleErrOnTwo, func(err error) { sunk <- err })
		_, ok := out.PullSafe()
		require.False(ok, "got a value, want closed")
		require.Empty(sunk)
	})
}

func TestTailReduce(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		head, tail := pipe.New[int](1)
		head.Close()
		require.Equal(t, 5, tail.Reduce(5, func(i, acc int) int { return acc + i }))
	})

	t.Run("sum", func(t *testing.T) {
		head, tail := pipe.New[int](4)
		for i := 1; i <= 4; i++ {
			head.Push(i)
		}
		head.Close()
		require.Equal(t, 10, tail.Reduce(0, func(i, acc int) int { return acc + i }))
	})

	t.Run("callOrder", func(t *testing.T) {
		head, tail := pipe.New[int](3)
		head.Push(1)
		head.Push(2)
		head.Push(3)
		head.Close()
		got := tail.Reduce(nil, func(i int, acc []int) []int { return append(acc, i) })
		require.Equal(t, []int{1, 2, 3}, got)
	})
}

func TestTailReduceAndEmit(t *testing.T) {
	t.Run("sum", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](3)
		head.Push(1)
		head.Push(2)
		head.Push(3)
		head.Close()
		out := tail.ReduceAndEmit(0, func(i, acc int) int { return acc + i })
		require.Equal(1, cap(out))
		require.Equal(6, out.Pull())
		_, ok := out.PullSafe()
		require.False(ok, "second value, want closed")
	})

	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		out := tail.ReduceAndEmit(5, func(i, acc int) int { return acc + i })
		require.Equal(5, out.Pull())
		_, ok := out.PullSafe()
		require.False(ok, "second value, want closed")
	})
}

func TestTailWindow(t *testing.T) {
	t.Run("singleFinal", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](3)
		head.Push(1)
		head.Push(2)
		head.Push(3)
		head.Close()
		out := tail.Window(time.Hour, func() int { return 0 }, func(i, acc int) int { return acc + i })
		require.Equal(1, cap(out))
		require.Equal(6, out.Pull())
		_, ok := out.PullSafe()
		require.False(ok, "second value, want closed")
	})

	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		out := tail.Window(time.Hour, func() int { return 7 }, func(i, acc int) int { return acc + i })
		require.Equal(7, out.Pull())
		_, ok := out.PullSafe()
		require.False(ok, "second value, want closed")
	})

	t.Run("ticks", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			require := require.New(t)
			head, tail := pipe.New[int](4)
			out := tail.Window(10*time.Millisecond, func() int { return 0 }, func(i, acc int) int { return acc + i })
			head.Push(1)
			head.Push(2)
			synctest.Wait()
			time.Sleep(10 * time.Millisecond)
			synctest.Wait()
			require.Equal(3, out.Pull(), "first window")
			head.Push(1)
			head.Close()
			synctest.Wait()
			require.Equal(1, out.Pull(), "final window")
			_, ok := out.PullSafe()
			require.False(ok, "value after final window, want closed")
		})
	})

	t.Run("emptyTick", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			require := require.New(t)
			accCalls := 0
			head, tail := pipe.New[int](1)
			out := tail.Window(10*time.Millisecond, func() int { accCalls++; return 7 }, func(i, acc int) int { return acc + i })
			time.Sleep(10 * time.Millisecond)
			synctest.Wait()
			require.Equal(7, out.Pull(), "tick")
			head.Close()
			synctest.Wait()
			require.Equal(7, out.Pull(), "final")
			_, ok := out.PullSafe()
			require.False(ok, "value after final window, want closed")
			require.Equal(2, accCalls, "acc calls for 2 emissions")
		})
	})
}

func TestTailRouter(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		routes, orElse := tail.Router([]int{1, 2}, func(i int) int { return i })
		require.Len(routes, 2)
		for i, route := range routes {
			_, ok := route.PullSafe()
			require.False(ok, "route %d got a value, want closed", i)
		}
		_, ok := orElse.PullSafe()
		require.False(ok, "orElse got a value, want closed")
	})

	t.Run("routes", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](4)
		head.Push(1)
		head.Push(2)
		head.Push(3)
		head.Push(1)
		head.Close()
		routes, orElse := tail.Router([]int{1, 2}, func(i int) int { return i })
		require.Len(routes, 2)
		require.Equal(4, cap(routes[0]))
		require.Equal(4, cap(routes[1]))
		require.Equal(4, cap(orElse))
		var ones, twos, rest []int
		for v := range routes[0] {
			ones = append(ones, v)
		}
		for v := range routes[1] {
			twos = append(twos, v)
		}
		for v := range orElse {
			rest = append(rest, v)
		}
		require.Equal([]int{1, 1}, ones)
		require.Equal([]int{2}, twos)
		require.Equal([]int{3}, rest)
	})

	t.Run("matchOrder", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](2)
		head.Push(1)
		head.Push(2)
		head.Close()
		routes, orElse := tail.Router([]int{2, 1}, func(i int) int { return i })
		require.Equal(2, routes[0].Pull(), "route 0")
		require.Equal(1, routes[1].Pull(), "route 1")
		v, ok := orElse.PullSafe()
		require.False(ok, "orElse got %d, want closed", v)
	})

	t.Run("noMatches", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](3)
		head.Push(1)
		head.Push(2)
		head.Push(3)
		head.Close()
		routes, orElse := tail.Router(nil, func(i int) int { return i })
		require.Empty(routes)
		var rest []int
		for v := range orElse {
			rest = append(rest, v)
		}
		require.Equal([]int{1, 2, 3}, rest)
	})

	t.Run("duplicate", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](3)
		head.Push(1)
		head.Push(1)
		head.Push(2)
		head.Close()
		routes, orElse := tail.Router([]int{1, 1}, func(i int) int { return i })
		require.Len(routes, 2)
		var last, rest []int
		for v := range routes[1] {
			last = append(last, v)
		}
		for v := range orElse {
			rest = append(rest, v)
		}
		require.Equal([]int{1, 1}, last)
		require.Equal([]int{2}, rest)
		_, ok := routes[0].TryPull()
		require.False(ok, "first duplicate route received a value")
	})
}

func TestTailRouterAsync(t *testing.T) {
	for _, workers := range []int{0, 4} {
		t.Run(fmt.Sprintf("workers%d", workers), func(t *testing.T) {
			require := require.New(t)
			head, tail := pipe.New[int](4)
			head.Push(1)
			head.Push(2)
			head.Push(3)
			head.Push(1)
			head.Close()
			routes, orElse := tail.RouterAsync(workers, []int{1, 2}, func(i int) int { return i })
			require.Len(routes, 2)
			require.Equal(4, cap(routes[0]))
			require.Equal(4, cap(routes[1]))
			require.Equal(4, cap(orElse))
			var ones, twos, rest []int
			for v := range routes[0] {
				ones = append(ones, v)
			}
			for v := range routes[1] {
				twos = append(twos, v)
			}
			for v := range orElse {
				rest = append(rest, v)
			}
			require.Equal([]int{1, 1}, ones)
			require.Equal([]int{2}, twos)
			require.Equal([]int{3}, rest)
		})
	}

	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		routes, orElse := tail.RouterAsync(4, []int{1, 2}, func(i int) int { return i })
		require.Len(routes, 2)
		for i, route := range routes {
			_, ok := route.PullSafe()
			require.False(ok, "route %d got a value, want closed", i)
		}
		_, ok := orElse.PullSafe()
		require.False(ok, "orElse got a value, want closed")
	})

	t.Run("duplicate", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](3)
		head.Push(1)
		head.Push(1)
		head.Push(2)
		head.Close()
		routes, orElse := tail.RouterAsync(4, []int{1, 1}, func(i int) int { return i })
		require.Len(routes, 2)
		var last, rest []int
		for v := range routes[1] {
			last = append(last, v)
		}
		for v := range orElse {
			rest = append(rest, v)
		}
		require.Equal([]int{1, 1}, last)
		require.Equal([]int{2}, rest)
		_, ok := routes[0].TryPull()
		require.False(ok, "first duplicate route received a value")
	})
}

func TestTailRouterWithSink(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		var sunk []int
		routes := tail.RouterWithSink([]int{1, 2}, func(i int) int { return i }, func(i int) { sunk = append(sunk, i) })
		require.Len(routes, 2)
		for i, route := range routes {
			_, ok := route.PullSafe()
			require.False(ok, "route %d got a value, want closed", i)
		}
		require.Empty(sunk)
	})

	t.Run("routes", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](4)
		head.Push(1)
		head.Push(2)
		head.Push(3)
		head.Push(1)
		head.Close()
		var sunk []int
		routes := tail.RouterWithSink([]int{1, 2}, func(i int) int { return i }, func(i int) { sunk = append(sunk, i) })
		require.Len(routes, 2)
		require.Equal(4, cap(routes[0]))
		require.Equal(4, cap(routes[1]))
		var ones, twos []int
		for v := range routes[0] {
			ones = append(ones, v)
		}
		for v := range routes[1] {
			twos = append(twos, v)
		}
		require.Equal([]int{1, 1}, ones)
		require.Equal([]int{2}, twos)
		require.Equal([]int{3}, sunk)
	})

	t.Run("duplicate", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](3)
		head.Push(1)
		head.Push(1)
		head.Push(2)
		head.Close()
		var sunk []int
		routes := tail.RouterWithSink([]int{1, 1}, func(i int) int { return i }, func(i int) { sunk = append(sunk, i) })
		require.Len(routes, 2)
		var last []int
		for v := range routes[1] {
			last = append(last, v)
		}
		require.Equal([]int{1, 1}, last)
		require.Equal([]int{2}, sunk)
		_, ok := routes[0].TryPull()
		require.False(ok, "first duplicate route received a value")
	})
}

func TestTailRouterWithSinkAsync(t *testing.T) {
	for _, workers := range []int{0, 4} {
		t.Run(fmt.Sprintf("workers%d", workers), func(t *testing.T) {
			require := require.New(t)
			head, tail := pipe.New[int](4)
			head.Push(1)
			head.Push(2)
			head.Push(3)
			head.Push(1)
			head.Close()
			sunk := make(chan int, 4)
			routes := tail.RouterWithSinkAsync(workers, []int{1, 2}, func(i int) int { return i }, func(i int) { sunk <- i })
			require.Len(routes, 2)
			require.Equal(4, cap(routes[0]))
			require.Equal(4, cap(routes[1]))
			var ones, twos []int
			for v := range routes[0] {
				ones = append(ones, v)
			}
			for v := range routes[1] {
				twos = append(twos, v)
			}
			require.Equal([]int{1, 1}, ones)
			require.Equal([]int{2}, twos)
			require.Len(sunk, 1)
			require.Equal(3, <-sunk)
		})
	}

	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		sunk := make(chan int, 1)
		routes := tail.RouterWithSinkAsync(4, []int{1, 2}, func(i int) int { return i }, func(i int) { sunk <- i })
		require.Len(routes, 2)
		for i, route := range routes {
			_, ok := route.PullSafe()
			require.False(ok, "route %d got a value, want closed", i)
		}
		require.Empty(sunk)
	})

	t.Run("duplicate", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](3)
		head.Push(1)
		head.Push(1)
		head.Push(2)
		head.Close()
		sunk := make(chan int, 3)
		routes := tail.RouterWithSinkAsync(4, []int{1, 1}, func(i int) int { return i }, func(i int) { sunk <- i })
		require.Len(routes, 2)
		var last []int
		for v := range routes[1] {
			last = append(last, v)
		}
		require.Equal([]int{1, 1}, last)
		require.Len(sunk, 1)
		require.Equal(2, <-sunk)
		_, ok := routes[0].TryPull()
		require.False(ok, "first duplicate route received a value")
	})
}

func TestTailRoundRobin(t *testing.T) {
	for _, count := range []int{0, -1} {
		t.Run(fmt.Sprintf("count%d", count), func(t *testing.T) {
			_, tail := pipe.New[int](1)
			require.Nil(t, tail.RoundRobin(count))
		})
	}

	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		tails := tail.RoundRobin(2)
		require.Len(tails, 2)
		for i, tl := range tails {
			_, ok := tl.PullSafe()
			require.False(ok, "tail %d got a value, want closed", i)
		}
	})

	t.Run("three", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](7)
		for i := range 7 {
			head.Push(i)
		}
		head.Close()
		tails := tail.RoundRobin(3)
		require.Len(tails, 3)
		want := [][]int{{0, 3, 6}, {1, 4}, {2, 5}}
		for i, tl := range tails {
			require.Equal(7, cap(tl), "tail %d cap", i)
			var got []int
			for v := range tl {
				got = append(got, v)
			}
			require.Equal(want[i], got, "tail %d", i)
		}
	})
}

func TestTailDistribute(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		_, tail := pipe.New[int](1)
		require.Nil(t, tail.Distribute(0, func(i int) int { return 0 }))
	})

	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		tails := tail.Distribute(2, func(i int) int { return i % 2 })
		require.Len(tails, 2)
		for i, tl := range tails {
			_, ok := tl.PullSafe()
			require.False(ok, "tail %d got a value, want closed", i)
		}
	})

	t.Run("parity", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](6)
		for i := 1; i <= 6; i++ {
			head.Push(i)
		}
		head.Close()
		tails := tail.Distribute(2, func(i int) int { return i % 2 })
		require.Len(tails, 2)
		require.Equal(6, cap(tails[0]))
		require.Equal(6, cap(tails[1]))
		var evens, odds []int
		for v := range tails[0] {
			evens = append(evens, v)
		}
		for v := range tails[1] {
			odds = append(odds, v)
		}
		require.Equal([]int{2, 4, 6}, evens)
		require.Equal([]int{1, 3, 5}, odds)
	})
}

func TestTailDistributeAsync(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		_, tail := pipe.New[int](1)
		require.Nil(t, tail.DistributeAsync(4, 0, func(i int) int { return 0 }))
	})

	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		tails := tail.DistributeAsync(4, 2, func(i int) int { return i % 2 })
		require.Len(tails, 2)
		for i, tl := range tails {
			_, ok := tl.PullSafe()
			require.False(ok, "tail %d got a value, want closed", i)
		}
	})

	for _, workers := range []int{0, 4} {
		t.Run(fmt.Sprintf("workers%d", workers), func(t *testing.T) {
			require := require.New(t)
			head, tail := pipe.New[int](6)
			for i := 1; i <= 6; i++ {
				head.Push(i)
			}
			head.Close()
			tails := tail.DistributeAsync(workers, 2, func(i int) int { return i % 2 })
			require.Len(tails, 2)
			require.Equal(6, cap(tails[0]))
			require.Equal(6, cap(tails[1]))
			var evens, odds []int
			for v := range tails[0] {
				evens = append(evens, v)
			}
			for v := range tails[1] {
				odds = append(odds, v)
			}
			slices.Sort(evens)
			slices.Sort(odds)
			require.Equal([]int{2, 4, 6}, evens)
			require.Equal([]int{1, 3, 5}, odds)
		})
	}
}

func TestTailSink(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		head, tail := pipe.New[int](1)
		head.Close()
		calls := 0
		tail.Sink(func(int) { calls++ })
		require.Equal(t, 0, calls, "sink calls")
	})

	t.Run("order", func(t *testing.T) {
		head, tail := pipe.New[int](3)
		head.Push(1)
		head.Push(2)
		head.Push(3)
		head.Close()
		var got []int
		tail.Sink(func(i int) { got = append(got, i) })
		require.Equal(t, []int{1, 2, 3}, got)
	})
}

func TestTailSinkAsync(t *testing.T) {
	for _, workers := range []int{0, 4} {
		t.Run(fmt.Sprintf("workers%d", workers), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				head, tail := pipe.New[int](3)
				head.Push(1)
				head.Push(2)
				head.Push(3)
				seen := make(chan int, 3)
				tail.SinkAsync(workers, func(i int) { seen <- i })
				head.Close()
				synctest.Wait()
				var got []int
				for len(seen) > 0 {
					got = append(got, <-seen)
				}
				slices.Sort(got)
				require.Equal(t, []int{1, 2, 3}, got)
			})
		})
	}

	t.Run("empty", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			head, tail := pipe.New[int](1)
			head.Close()
			calls := 0
			tail.SinkAsync(4, func(int) { calls++ })
			synctest.Wait()
			require.Equal(t, 0, calls, "sink calls")
		})
	})
}

// errOnEven returns errBoom for even values.
func errOnEven(i int) error {
	if i%2 == 0 {
		return errBoom
	}
	return nil
}

func TestTailSinkError(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		head, tail := pipe.New[int](1)
		head.Close()
		errs := tail.SinkError(errOnEven)
		_, ok := errs.PullSafe()
		require.False(t, ok, "got an error, want closed")
	})

	t.Run("errors", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](4)
		for i := 1; i <= 4; i++ {
			head.Push(i)
		}
		head.Close()
		errs := tail.SinkError(errOnEven)
		require.Equal(4, cap(errs))
		var got []error
		for err := range errs {
			got = append(got, err)
		}
		require.Equal([]error{errBoom, errBoom}, got)
	})
}

func TestTailSinkErrorAsync(t *testing.T) {
	for _, workers := range []int{0, 4} {
		t.Run(fmt.Sprintf("workers%d", workers), func(t *testing.T) {
			require := require.New(t)
			head, tail := pipe.New[int](4)
			for i := 1; i <= 4; i++ {
				head.Push(i)
			}
			head.Close()
			errs := tail.SinkErrorAsync(workers, errOnEven)
			require.Equal(4, cap(errs))
			var got []error
			for err := range errs {
				got = append(got, err)
			}
			require.Equal([]error{errBoom, errBoom}, got)
		})
	}

	t.Run("empty", func(t *testing.T) {
		head, tail := pipe.New[int](1)
		head.Close()
		errs := tail.SinkErrorAsync(4, errOnEven)
		_, ok := errs.PullSafe()
		require.False(t, ok, "got an error, want closed")
	})
}

func TestTailSinkErrorSink(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		calls := 0
		var sunk []error
		tail.SinkErrorSink(func(int) error { calls++; return errBoom }, func(err error) { sunk = append(sunk, err) })
		require.Equal(0, calls, "sink calls")
		require.Empty(sunk)
	})

	t.Run("errors", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](4)
		for i := 1; i <= 4; i++ {
			head.Push(i)
		}
		head.Close()
		var seen []int
		var sunk []error
		tail.SinkErrorSink(func(i int) error {
			seen = append(seen, i)
			return errOnEven(i)
		}, func(err error) { sunk = append(sunk, err) })
		require.Equal([]int{1, 2, 3, 4}, seen)
		require.Equal([]error{errBoom, errBoom}, sunk)
	})
}

func TestTailSinkErrorSinkAsync(t *testing.T) {
	for _, workers := range []int{0, 4} {
		t.Run(fmt.Sprintf("workers%d", workers), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				require := require.New(t)
				head, tail := pipe.New[int](4)
				for i := 1; i <= 4; i++ {
					head.Push(i)
				}
				seen := make(chan int, 4)
				sunk := make(chan error, 4)
				tail.SinkErrorSinkAsync(workers, func(i int) error {
					seen <- i
					return errOnEven(i)
				}, func(err error) { sunk <- err })
				head.Close()
				synctest.Wait()
				var got []int
				for len(seen) > 0 {
					got = append(got, <-seen)
				}
				slices.Sort(got)
				require.Equal([]int{1, 2, 3, 4}, got)
				require.Len(sunk, 2)
			})
		})
	}

	t.Run("empty", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			require := require.New(t)
			head, tail := pipe.New[int](1)
			head.Close()
			calls := 0
			var sunk []error
			tail.SinkErrorSinkAsync(4, func(int) error { calls++; return errBoom }, func(err error) { sunk = append(sunk, err) })
			synctest.Wait()
			require.Equal(0, calls, "sink calls")
			require.Empty(sunk)
		})
	})
}

func TestTailTap(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		calls := 0
		out := tail.Tap(func(int) { calls++ })
		_, ok := out.PullSafe()
		require.False(ok, "got a value, want closed")
		require.Equal(0, calls, "tap calls")
	})

	t.Run("order", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](3)
		head.Push(1)
		head.Push(2)
		head.Push(3)
		head.Close()
		var seen []int
		out := tail.Tap(func(i int) { seen = append(seen, i) })
		require.Equal(3, cap(out))
		var got []int
		for v := range out {
			got = append(got, v)
		}
		require.Equal([]int{1, 2, 3}, got)
		require.Equal([]int{1, 2, 3}, seen)
	})

	t.Run("tapPrecedesForward", func(t *testing.T) {
		head, tail := pipe.New[int](3)
		head.Push(1)
		head.Push(2)
		head.Push(3)
		head.Close()
		var seen []int
		out := tail.Tap(func(i int) { seen = append(seen, i) })
		for v := range out {
			require.Contains(t, seen, v, "pulled %d before tap saw it", v)
		}
	})
}

func TestTailTapAsync(t *testing.T) {
	for _, workers := range []int{0, 4} {
		t.Run(fmt.Sprintf("workers%d", workers), func(t *testing.T) {
			require := require.New(t)
			head, tail := pipe.New[int](3)
			head.Push(1)
			head.Push(2)
			head.Push(3)
			head.Close()
			seen := make(chan int, 3)
			out := tail.TapAsync(workers, func(i int) { seen <- i })
			require.Equal(3, cap(out))
			var got []int
			for v := range out {
				got = append(got, v)
			}
			var tapped []int
			for len(seen) > 0 {
				tapped = append(tapped, <-seen)
			}
			slices.Sort(got)
			slices.Sort(tapped)
			require.Equal([]int{1, 2, 3}, got)
			require.Equal([]int{1, 2, 3}, tapped)
		})
	}

	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		seen := make(chan int, 1)
		out := tail.TapAsync(4, func(i int) { seen <- i })
		_, ok := out.PullSafe()
		require.False(ok, "got a value, want closed")
		require.Empty(seen)
	})
}

func TestTailTapError(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		out, errs := tail.TapError(errOnEven)
		_, ok := out.PullSafe()
		require.False(ok, "got a value, want closed")
		_, ok = errs.PullSafe()
		require.False(ok, "got an error, want closed")
	})

	t.Run("forwardsAll", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](3)
		head.Push(1)
		head.Push(2)
		head.Push(3)
		head.Close()
		out, errs := tail.TapError(errOnEven)
		require.Equal(3, cap(out))
		require.Equal(3, cap(errs))
		var got []int
		for v := range out {
			got = append(got, v)
		}
		var gotErrs []error
		for err := range errs {
			gotErrs = append(gotErrs, err)
		}
		require.Equal([]int{1, 2, 3}, got)
		require.Equal([]error{errBoom}, gotErrs)
	})
}

func TestTailTapErrorAsync(t *testing.T) {
	for _, workers := range []int{0, 4} {
		t.Run(fmt.Sprintf("workers%d", workers), func(t *testing.T) {
			require := require.New(t)
			head, tail := pipe.New[int](3)
			head.Push(1)
			head.Push(2)
			head.Push(3)
			head.Close()
			out, errs := tail.TapErrorAsync(workers, errOnEven)
			require.Equal(3, cap(out))
			require.Equal(3, cap(errs))
			var got []int
			for v := range out {
				got = append(got, v)
			}
			var gotErrs []error
			for err := range errs {
				gotErrs = append(gotErrs, err)
			}
			slices.Sort(got)
			require.Equal([]int{1, 2, 3}, got)
			require.Equal([]error{errBoom}, gotErrs)
		})
	}

	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		out, errs := tail.TapErrorAsync(4, errOnEven)
		_, ok := out.PullSafe()
		require.False(ok, "got a value, want closed")
		_, ok = errs.PullSafe()
		require.False(ok, "got an error, want closed")
	})
}

func TestTailTapErrorSink(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		var sunk []error
		out := tail.TapErrorSink(errOnEven, func(err error) { sunk = append(sunk, err) })
		_, ok := out.PullSafe()
		require.False(ok, "got a value, want closed")
		require.Empty(sunk)
	})

	t.Run("forwardsAll", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](3)
		head.Push(1)
		head.Push(2)
		head.Push(3)
		head.Close()
		var sunk []error
		out := tail.TapErrorSink(errOnEven, func(err error) { sunk = append(sunk, err) })
		require.Equal(3, cap(out))
		var got []int
		for v := range out {
			got = append(got, v)
		}
		require.Equal([]int{1, 2, 3}, got)
		require.Equal([]error{errBoom}, sunk)
	})
}

func TestTailTapErrorSinkAsync(t *testing.T) {
	for _, workers := range []int{0, 4} {
		t.Run(fmt.Sprintf("workers%d", workers), func(t *testing.T) {
			require := require.New(t)
			head, tail := pipe.New[int](3)
			head.Push(1)
			head.Push(2)
			head.Push(3)
			head.Close()
			sunk := make(chan error, 3)
			out := tail.TapErrorSinkAsync(workers, errOnEven, func(err error) { sunk <- err })
			require.Equal(3, cap(out))
			var got []int
			for v := range out {
				got = append(got, v)
			}
			slices.Sort(got)
			require.Equal([]int{1, 2, 3}, got)
			require.Len(sunk, 1)
			require.Equal(errBoom, <-sunk)
		})
	}

	t.Run("empty", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Close()
		sunk := make(chan error, 1)
		out := tail.TapErrorSinkAsync(4, errOnEven, func(err error) { sunk <- err })
		_, ok := out.PullSafe()
		require.False(ok, "got a value, want closed")
		require.Empty(sunk)
	})
}

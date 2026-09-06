package pipe_test

import (
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/Morgahl/pipe"
)

func TestNew(t *testing.T) {
	t.Run("unbuffered", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](0)
		require.Equal(0, cap(head))
		require.Equal(0, cap(tail))
		require.False(head.TryPush(1), "TryPush with no receiver")
	})

	t.Run("buffered", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](3)
		require.Equal(3, cap(head))
		require.Equal(3, cap(tail))
		for i := range 3 {
			require.True(head.TryPush(i), "TryPush %d", i)
		}
		require.False(head.TryPush(3), "TryPush on full channel")
	})

	t.Run("shared", func(t *testing.T) {
		head, tail := pipe.New[int](1)
		head.Push(7)
		require.Equal(t, 7, tail.Pull())
	})
}

func TestHeadClose(t *testing.T) {
	head, tail := pipe.New[int](1)
	head.Close()
	_, ok := tail.PullSafe()
	require.False(t, ok, "PullSafe on closed channel")
}

func TestHeadPush(t *testing.T) {
	t.Run("buffered", func(t *testing.T) {
		head, tail := pipe.New[int](3)
		head.Push(1)
		head.Push(2)
		head.Push(3)
		head.Close()
		var got []int
		for v := range tail {
			got = append(got, v)
		}
		require.Equal(t, []int{1, 2, 3}, got)
	})

	t.Run("unbuffered", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			head, tail := pipe.New[int](0)
			var got int
			go func() { got = tail.Pull() }()
			head.Push(7)
			synctest.Wait()
			require.Equal(t, 7, got)
		})
	})

	t.Run("blocksWhenFull", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			require := require.New(t)
			head, tail := pipe.New[int](1)
			head.Push(1)
			done := false
			go func() {
				head.Push(2)
				done = true
			}()
			synctest.Wait()
			require.False(done, "Push returned on a full channel")
			require.Equal(1, tail.Pull())
			synctest.Wait()
			require.True(done, "Push blocked after space freed")
			require.Equal(2, tail.Pull())
		})
	})
}

func TestHeadTryPush(t *testing.T) {
	t.Run("space", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		require.True(head.TryPush(5), "TryPush with space")
		require.Equal(5, tail.Pull())
	})

	t.Run("full", func(t *testing.T) {
		require := require.New(t)
		head, tail := pipe.New[int](1)
		head.Push(1)
		require.False(head.TryPush(2), "TryPush on full channel")
		require.Equal(1, tail.Pull())
	})

	t.Run("unbufferedNoReceiver", func(t *testing.T) {
		head, _ := pipe.New[int](0)
		require.False(t, head.TryPush(1), "TryPush with no receiver")
	})

	t.Run("unbufferedWaitingReceiver", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			require := require.New(t)
			head, tail := pipe.New[int](0)
			var got int
			var ok bool
			go func() { got, ok = tail.PullSafe() }()
			synctest.Wait()
			require.True(head.TryPush(9), "TryPush with waiting receiver")
			synctest.Wait()
			require.True(ok)
			require.Equal(9, got)
		})
	})

	t.Run("nil", func(t *testing.T) {
		var head pipe.Head[int]
		require.False(t, head.TryPush(1), "TryPush on nil channel")
	})
}

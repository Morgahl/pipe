package pipe

import "time"

// Tail is the pull only end of a channel. This is essentially a <-chan T and can be used the same
// way as one would use a receive only channel in Go under normal syntax usages. However this variant
// has methods for pulling from the channel and for functional operations common to the use and
// lifecycle of channels. A T can only be pulled from a Tail.
type Tail[T any] <-chan T

// New returns the [Head] and [Tail] of a new channel with the given type T. This is essentially a
// chan T split into its push only and pull only ends, each with methods for operations common to
// the use and lifecycle of channels.
//
// Passing a len of 0 will create an unbuffered channel.
func New[T any](len int) (Head[T], Tail[T]) {
	pipe := make(chan T, len)
	return Head[T](pipe), Tail[T](pipe)
}

// FanIn is a non-blocking operation that creates len(tails) goroutines and forwards each T pulled
// onto the returned tail only channel of specified size. Each goroutine will exit after its assigned
// tail only channel is closed and emptied. The last goroutine will close the returned tail only
// channel to signal completion of processing. Passing no tails returns a closed channel.
func FanIn[T any](size int, tails ...Tail[T]) Tail[T] {
	head, tail := New[T](size)
	if len(tails) < 1 {
		// Let's never return a nil channel, a close empty channel has better behaviors
		close(head)
		return tail
	}
	go fanInCoordinator(head, tails)
	return tail
}

// Source is a non-blocking operation that creates a goroutine calling source repeat times and
// pushing each T onto the returned tail only channel of specified size. Once repeat is exhausted the
// goroutine calls closer, closes the returned tail only channel and exits. Passing 0 or any other
// negative value calls source zero times, calls closer and closes the channel immeadiately. Passing
// [RepeatForever] will call source until the program exits.
func Source[T any, S func() T, C func()](repeat, size int, source S, closer C) Tail[T] {
	head, tail := New[T](size)
	go sourceWorker(head, repeat, source, closer)
	return tail
}

// SourceError is a non-blocking operation that behaves as [Source] but source may return an error.
// Each error is pushed onto the returned error tail only channel and no T is pushed for that call.
// Returning [Done] from source ends the goroutine as if repeat were exhausted and [Done] is not
// pushed. closer is called and both returned channels are closed once repeat is exhausted or [Done]
// is returned.
func SourceError[T any, S func() (T, error), C func()](repeat, size int, source S, closer C) (Tail[T], Tail[error]) {
	head, tail := New[T](size)
	err := make(chan error, size)
	go sourceErrorWorker(head, err, repeat, source, closer)
	return tail, err
}

// SourceErrorSink is a non-blocking operation that behaves as [SourceError] but each error is
// passed to sink instead of being pushed onto a channel. [Done] is not passed to sink.
func SourceErrorSink[T any, S func() (T, error), C func(), E func(error)](repeat, size int, source S, closer C, sink E) Tail[T] {
	head, tail := New[T](size)
	go sourceErrorSinkWorker(head, repeat, source, closer, sink)
	return tail
}

// Pull is a blocking operation that pulls a T from the channel if available. This blocks while no T
// is available. If the channel is closed and empty, or nil, this will return a zero version of the
// T type.
func (tl Tail[T]) Pull() T {
	return <-tl
}

// PullSafe is a blocking operation that pulls a T from the channel if available. This returns true
// if the T returned is valid, false if the channel is closed and empty, or nil.
func (tl Tail[T]) PullSafe() (t T, ok bool) {
	t, ok = <-tl
	return
}

// TryPull is a non-blocking operation that attempts to pull a T from the channel. This returns true
// if the T returned is valid, false if the channel is closed and empty, or nil.
func (tl Tail[T]) TryPull() (t T, ok bool) {
	select {
	case t, ok = <-tl:
	default: // chan is empty or closed
	}
	return
}

// Drain is a blocking operation that iterates over the channel discarding values until the channel
// is closed and no further elements remain. This returns immeadiately if the channel is closed or
// nil.
func (tl Tail[T]) Drain() {
	for range tl {
	}
}

// Wait is a blocking operation that waits for a value to be returned from the channel. If the
// channel is closed or nil this will immeadiately return.
func (tl Tail[T]) Wait() {
	<-tl
}

// FanOut is a non-blocking operation that creates count tail only channels of the same size as this
// channel and forwards every T pulled onto each of them. All returned channels are closed after this
// channel is closed and emptied.
func (tl Tail[T]) FanOut(count int) []Tail[T] {
	tails := make([]Tail[T], count)
	fan := make([]Head[T], count)
	for i := range tails {
		fan[i], tails[i] = New[T](cap(tl))
	}
	go fanOutWorker(tl, fan...)
	return tails
}

// Filter is a non-blocking operation that forwards each T for which filter returns true onto the
// returned tail only channel of the same size as this channel. The returned channel is closed after
// this channel is closed and emptied.
func (tl Tail[T]) Filter[F func(T) bool](filter F) Tail[T] {
	head, tail := New[T](cap(tl))
	go filterWorker(tl, head, filter)
	return tail
}

// FilterAsync is a non-blocking operation that behaves as [Tail.Filter] but runs filter across
// workers goroutines. At least one goroutine is always created. The returned channel is closed
// after the last goroutine exits.
//
// Ordering is lost through FilterAsync. Each goroutine pulls from this channel and pushes onto the
// returned channel independently, so values are emitted in the order filter finishes, not the order
// they were pulled.
func (tl Tail[T]) FilterAsync[F func(T) bool](workers int, filter F) Tail[T] {
	head, tail := New[T](cap(tl))
	go filterAsyncCoordinator(tl, head, workers, filter)
	return tail
}

// FilterError is a non-blocking operation that behaves as [Tail.Filter] but filter may return an
// error. Each error is pushed onto the returned error tail only channel and the T is discarded.
// Both returned channels are closed after this channel is closed and emptied.
func (tl Tail[T]) FilterError[F func(T) (bool, error)](filter F) (Tail[T], Tail[error]) {
	head, tail := New[T](cap(tl))
	headErr, tailErr := New[error](cap(tl))
	go filterErrorWorker(tl, head, headErr, filter)
	return tail, tailErr
}

// FilterErrorAsync is a non-blocking operation that behaves as [Tail.FilterError] but runs filter
// across workers goroutines. At least one goroutine is always created. Both returned channels are
// closed after the last goroutine exits.
//
// Ordering is lost through FilterErrorAsync. Each goroutine pulls from this channel and pushes onto
// the returned channels independently, so values and errors are emitted in the order filter
// finishes, not the order they were pulled.
func (tl Tail[T]) FilterErrorAsync[F func(T) (bool, error)](workers int, filter F) (Tail[T], Tail[error]) {
	head, tail := New[T](cap(tl))
	headErr, tailErr := New[error](cap(tl))
	go filterErrorAsyncCoordinator(tl, head, headErr, workers, filter)
	return tail, tailErr
}

// FilterErrorSink is a non-blocking operation that behaves as [Tail.FilterError] but each error is
// passed to sink instead of being pushed onto a channel.
func (tl Tail[T]) FilterErrorSink[F func(T) (bool, error), S func(error)](filter F, sink S) Tail[T] {
	head, tail := New[T](cap(tl))
	go filterErrorSinkWorker(tl, head, filter, sink)
	return tail
}

// FilterErrorSinkAsync is a non-blocking operation that behaves as [Tail.FilterErrorSink] but runs
// filter across workers goroutines. At least one goroutine is always created. The returned channel
// is closed after the last goroutine exits.
//
// Ordering is lost through FilterErrorSinkAsync. Each goroutine pulls from this channel, pushes onto
// the returned channel and calls sink independently, so values are emitted and errors sunk in the
// order filter finishes, not the order they were pulled.
func (tl Tail[T]) FilterErrorSinkAsync[F func(T) (bool, error), S func(error)](workers int, filter F, sink S) Tail[T] {
	head, tail := New[T](cap(tl))
	go filterErrorSinkAsyncCoordinator(tl, head, workers, filter, sink)
	return tail
}

// Map is a non-blocking operation that pushes the result of mp for each T onto the returned tail
// only channel of the same size as this channel. The returned channel is closed after this channel
// is closed and emptied.
func (tl Tail[T]) Map[U any, M func(T) U](mp M) Tail[U] {
	head, tail := New[U](cap(tl))
	go mapWorker(tl, head, mp)
	return tail
}

// MapAsync is a non-blocking operation that behaves as [Tail.Map] but runs mp across workers
// goroutines. At least one goroutine is always created. The returned channel is closed after the
// last goroutine exits.
//
// Ordering is lost through MapAsync. Each goroutine pulls from this channel and pushes onto the
// returned channel independently, so values are emitted in the order mp finishes, not the order
// they were pulled.
func (tl Tail[T]) MapAsync[U any, M func(T) U](workers int, mp M) Tail[U] {
	head, tail := New[U](cap(tl))
	go mapAsyncCoordinator(tl, head, workers, mp)
	return tail
}

// MapError is a non-blocking operation that behaves as [Tail.Map] but mp may return an error. Each
// error is pushed onto the returned error tail only channel and no U is pushed for that T. Both
// returned channels are closed after this channel is closed and emptied.
func (tl Tail[T]) MapError[U any, M func(T) (U, error)](mp M) (Tail[U], Tail[error]) {
	head, tail := New[U](cap(tl))
	err := make(chan error, cap(tl))
	go mapErrorWorker(tl, head, err, mp)
	return tail, err
}

// MapErrorAsync is a non-blocking operation that behaves as [Tail.MapError] but runs mp across
// workers goroutines. At least one goroutine is always created. Both returned channels are closed
// after the last goroutine exits.
//
// Ordering is lost through MapErrorAsync. Each goroutine pulls from this channel and pushes onto the
// returned channels independently, so values and errors are emitted in the order mp finishes, not
// the order they were pulled.
func (tl Tail[T]) MapErrorAsync[U any, M func(T) (U, error)](workers int, mp M) (Tail[U], Tail[error]) {
	head, tail := New[U](cap(tl))
	err := make(chan error, cap(tl))
	go mapErrorAsyncCoordinator(tl, head, err, workers, mp)
	return tail, err
}

// MapErrorSink is a non-blocking operation that behaves as [Tail.MapError] but each error is passed
// to sink instead of being pushed onto a channel.
func (tl Tail[T]) MapErrorSink[U any, M func(T) (U, error), S func(error)](mp M, sink S) Tail[U] {
	head, tail := New[U](cap(tl))
	go mapErrorSinkWorker(tl, head, mp, sink)
	return tail
}

// MapErrorSinkAsync is a non-blocking operation that behaves as [Tail.MapErrorSink] but runs mp
// across workers goroutines. At least one goroutine is always created. The returned channel is
// closed after the last goroutine exits.
//
// Ordering is lost through MapErrorSinkAsync. Each goroutine pulls from this channel, pushes onto
// the returned channel and calls sink independently, so values are emitted and errors sunk in the
// order mp finishes, not the order they were pulled.
func (tl Tail[T]) MapErrorSinkAsync[U any, M func(T) (U, error), S func(error)](workers int, mp M, sink S) Tail[U] {
	head, tail := New[U](cap(tl))
	go mapErrorSinkAsyncCoordinator(tl, head, workers, mp, sink)
	return tail
}

// Reduce is a blocking operation that calls reduce with each T pulled and the current accumulator,
// starting from acc, and returns the final accumulator once this channel is closed and emptied.
func (tl Tail[T]) Reduce[Acc any, R func(T, Acc) Acc](acc Acc, reduce R) Acc {
	for t := range tl {
		acc = reduce(t, acc)
	}
	return acc
}

// ReduceAndEmit is a non-blocking operation that behaves as [Tail.Reduce] but pushes the final
// accumulator onto the returned tail only channel of size 1 instead of returning it. The returned
// channel is closed after the accumulator is pushed.
func (tl Tail[T]) ReduceAndEmit[Acc any, R func(T, Acc) Acc](acc Acc, reduce R) Tail[Acc] {
	// we only expect to emit a single value and then close the out chan immeadiately
	// after processing. This allows the goroutine to exit without forcing it to sync
	// with the recieving goroutine.
	head, tail := New[Acc](1)
	go reduceAndEmitWorker(tl, head, reduce, acc)
	return tail
}

// Window is a non-blocking operation that calls reduce with each T pulled and the current
// accumulator. Every window duration the accumulator is pushed onto the returned tail only channel
// of size 1 and replaced with a fresh one from acc. The final accumulator is pushed and the returned
// channel closed after this channel is closed and emptied.
func (tl Tail[T]) Window[Acc any, A func() Acc, R func(T, Acc) Acc](window time.Duration, acc A, reduce R) Tail[Acc] {
	head, tail := New[Acc](1)
	go windowWorker(tl, head, window, reduce, acc)
	return tail
}

// Router is a non-blocking operation that creates a tail only channel per match, each the same size
// as this channel, and forwards each T onto the route whose match equals compare(T). A T matching no
// route is forwarded onto orElse. Routes are returned in the order of matches. Duplicate matches each
// return a channel but only the last one created for that match receives values and is closed. All
// other returned channels are closed after this channel is closed and emptied.
func (tl Tail[T]) Router[Cmp comparable, C func(T) Cmp](matches []Cmp, compare C) (routes []Tail[T], orElse Tail[T]) {
	elsehead, elsetail := New[T](cap(tl))
	routes = make([]Tail[T], len(matches))
	mappedRoutes := make(map[Cmp]Head[T], len(matches))
	for i, match := range matches {
		mappedRoutes[match], routes[i] = New[T](cap(tl))
	}
	go routerWorker(tl, mappedRoutes, elsehead, compare)
	return routes, elsetail
}

// RouterAsync is a non-blocking operation that behaves as [Tail.Router] but runs compare across
// workers goroutines. At least one goroutine is always created. All returned channels are closed
// after the last goroutine exits.
//
// Ordering is lost through RouterAsync. Each goroutine pulls from this channel and pushes onto the
// routes and orElse independently, so values are emitted in the order compare finishes, not the
// order they were pulled.
func (tl Tail[T]) RouterAsync[Cmp comparable, C func(T) Cmp](workers int, matches []Cmp, compare C) (routes []Tail[T], orElse Tail[T]) {
	elsehead, elsetail := New[T](cap(tl))
	routes = make([]Tail[T], len(matches))
	mappedRoutes := make(map[Cmp]Head[T], len(matches))
	for i, match := range matches {
		mappedRoutes[match], routes[i] = New[T](cap(tl))
	}
	go routerAsyncCoordinator(tl, mappedRoutes, elsehead, workers, compare)
	return routes, elsetail
}

// RouterWithSink is a non-blocking operation that behaves as [Tail.Router] but a T matching no
// route is passed to sink instead of being forwarded onto a channel.
func (tl Tail[T]) RouterWithSink[Cmp comparable, C func(T) Cmp, S func(T)](matches []Cmp, compare C, sink S) (routes []Tail[T]) {
	routes = make([]Tail[T], len(matches))
	mappedRoutes := make(map[Cmp]Head[T], len(matches))
	for i, match := range matches {
		mappedRoutes[match], routes[i] = New[T](cap(tl))
	}
	go routerWithSinkWorker(tl, mappedRoutes, compare, sink)
	return routes
}

// RouterWithSinkAsync is a non-blocking operation that behaves as [Tail.RouterWithSink] but runs
// compare and sink across workers goroutines. At least one goroutine is always created. All
// returned channels are closed after the last goroutine exits.
//
// Ordering is lost through RouterWithSinkAsync. Each goroutine pulls from this channel, pushes onto
// the routes and calls sink independently, so values are emitted or sunk in the order compare
// finishes, not the order they were pulled.
func (tl Tail[T]) RouterWithSinkAsync[Cmp comparable, C func(T) Cmp, S func(T)](workers int, matches []Cmp, compare C, sink S) (routes []Tail[T]) {
	routes = make([]Tail[T], len(matches))
	mappedRoutes := make(map[Cmp]Head[T], len(matches))
	for i, match := range matches {
		mappedRoutes[match], routes[i] = New[T](cap(tl))
	}
	go routerWithSinkAsyncCoordinator(tl, mappedRoutes, workers, compare, sink)
	return routes
}

// RoundRobin is a non-blocking operation that behaves as [Tail.Distribute] with each T forwarded
// onto the next channel in turn. A count of less than 1 returns nil.
func (tl Tail[T]) RoundRobin(count int) []Tail[T] {
	if count < 1 {
		return nil
	}

	return tl.Distribute(count, roundRobinChooser[T](count))
}

// Distribute is a non-blocking operation that creates count tail only channels of the same size as
// this channel and forwards each T onto the channel at index choose(T). A count of less than 1
// returns nil. A choose result outside 0 to count-1 panics. All returned channels are closed after
// this channel is closed and emptied.
func (tl Tail[T]) Distribute[C func(T) int](count int, choose C) []Tail[T] {
	if count < 1 {
		return nil
	}

	tails := make([]Tail[T], count)
	heades := make([]Head[T], count)
	for i := 0; i < count; i++ {
		heades[i], tails[i] = New[T](cap(tl))
	}

	go distrbuteWorker(tl, heades, choose)

	return tails
}

// DistributeAsync is a non-blocking operation that behaves as [Tail.Distribute] but runs choose
// across workers goroutines. At least one goroutine is always created. choose is called
// concurrently and must be safe for concurrent use. All returned channels are closed after the last
// goroutine exits.
//
// Ordering is lost through DistributeAsync. Each goroutine pulls from this channel and pushes onto
// the returned channels independently, so values are emitted in the order choose finishes, not the
// order they were pulled.
func (tl Tail[T]) DistributeAsync[C func(T) int](workers int, count int, choose C) []Tail[T] {
	if count < 1 {
		return nil
	}

	tails := make([]Tail[T], count)
	heades := make([]Head[T], count)
	for i := 0; i < count; i++ {
		heades[i], tails[i] = New[T](cap(tl))
	}

	go distributeAsyncCoordinator(tl, heades, workers, choose)

	return tails
}

// Sink is a blocking operation that passes each T pulled to sink until this channel is closed and
// emptied.
func (tl Tail[T]) Sink[S func(T)](sink S) {
	for t := range tl {
		sink(t)
	}
}

// SinkAsync is a non-blocking operation that behaves as [Tail.Sink] but runs sink across workers
// goroutines. At least one goroutine is always created. The goroutines exit after this channel is
// closed and emptied.
//
// Ordering is lost through SinkAsync. Each goroutine pulls from this channel and calls sink
// independently, so sink calls overlap and finish in no fixed order.
func (tl Tail[T]) SinkAsync[S func(T)](workers int, sink S) {
	go sinkAsyncCoordinator(tl, workers, sink)
}

// SinkError is a non-blocking operation that creates a goroutine passing each T pulled to sink. Each
// error returned by sink is pushed onto the returned error tail only channel of the same size as
// this channel. The returned channel is closed after this channel is closed and emptied.
func (tl Tail[T]) SinkError[S func(T) error](sink S) Tail[error] {
	err := make(chan error, cap(tl))
	go sinkErrorWorker(tl, err, sink)
	return err
}

// SinkErrorAsync is a non-blocking operation that behaves as [Tail.SinkError] but runs sink across
// workers goroutines. At least one goroutine is always created. The returned channel is closed
// after the last goroutine exits.
//
// Ordering is lost through SinkErrorAsync. Each goroutine pulls from this channel and pushes onto
// the returned channel independently, so errors are emitted in the order sink finishes, not the
// order values were pulled.
func (tl Tail[T]) SinkErrorAsync[S func(T) error](workers int, sink S) Tail[error] {
	err := make(chan error, cap(tl))
	go sinkErrorAsyncCoordinator(tl, err, workers, sink)
	return err
}

// SinkErrorSink is a blocking operation that behaves as [Tail.Sink] but sink may return an error.
// Each error is passed to errSink.
func (tl Tail[T]) SinkErrorSink[S func(T) error, E func(error)](sink S, errSink E) {
	for t := range tl {
		if err := sink(t); err != nil {
			errSink(err)
		}
	}
}

// SinkErrorSinkAsync is a non-blocking operation that behaves as [Tail.SinkErrorSink] but runs sink
// and errSink across workers goroutines. At least one goroutine is always created. The goroutines
// exit after this channel is closed and emptied.
//
// Ordering is lost through SinkErrorSinkAsync. Each goroutine pulls from this channel and calls
// sink and errSink independently, so calls overlap and finish in no fixed order.
func (tl Tail[T]) SinkErrorSinkAsync[S func(T) error, E func(error)](workers int, sink S, errSink E) {
	go sinkErrorSinkAsyncCoordinator(tl, workers, sink, errSink)
}

// Tap is a non-blocking operation that passes each T pulled to tap and then forwards it onto the
// returned tail only channel of the same size as this channel. The returned channel is closed after
// this channel is closed and emptied.
func (tl Tail[T]) Tap[Tp func(T)](tap Tp) Tail[T] {
	head, tail := New[T](cap(tl))
	go tapWorker(tl, head, tap)
	return tail
}

// TapAsync is a non-blocking operation that behaves as [Tail.Tap] but runs tap across workers
// goroutines. At least one goroutine is always created. The returned channel is closed after the
// last goroutine exits.
//
// Ordering is lost through TapAsync. Each goroutine pulls from this channel and pushes onto the
// returned channel independently, so values are emitted in the order tap finishes, not the order
// they were pulled.
func (tl Tail[T]) TapAsync[Tp func(T)](workers int, tap Tp) Tail[T] {
	head, tail := New[T](cap(tl))
	go tapAsyncCoordinator(tl, head, workers, tap)
	return tail
}

// TapError is a non-blocking operation that behaves as [Tail.Tap] but tap may return an error. Each
// error is pushed onto the returned error tail only channel and the T is still forwarded. Both
// returned channels are closed after this channel is closed and emptied.
func (tl Tail[T]) TapError[Tp func(T) error](tap Tp) (Tail[T], Tail[error]) {
	head, tail := New[T](cap(tl))
	err := make(chan error, cap(tl))
	go tapErrorWorker(tl, head, err, tap)
	return tail, err
}

// TapErrorAsync is a non-blocking operation that behaves as [Tail.TapError] but runs tap across
// workers goroutines. At least one goroutine is always created. Both returned channels are closed
// after the last goroutine exits.
//
// Ordering is lost through TapErrorAsync. Each goroutine pulls from this channel and pushes onto the
// returned channels independently, so values and errors are emitted in the order tap finishes, not
// the order they were pulled.
func (tl Tail[T]) TapErrorAsync[Tp func(T) error](workers int, tap Tp) (Tail[T], Tail[error]) {
	head, tail := New[T](cap(tl))
	err := make(chan error, cap(tl))
	go tapErrorAsyncCoordinator(tl, head, err, workers, tap)
	return tail, err
}

// TapErrorSink is a non-blocking operation that behaves as [Tail.TapError] but each error is passed
// to sink instead of being pushed onto a channel.
func (tl Tail[T]) TapErrorSink[Tp func(T) error, S func(error)](tap Tp, sink S) Tail[T] {
	head, tail := New[T](cap(tl))
	go tapErrorSinkWorker(tl, head, tap, sink)
	return tail
}

// TapErrorSinkAsync is a non-blocking operation that behaves as [Tail.TapErrorSink] but runs tap
// across workers goroutines. At least one goroutine is always created. The returned channel is
// closed after the last goroutine exits.
//
// Ordering is lost through TapErrorSinkAsync. Each goroutine pulls from this channel, pushes onto
// the returned channel and calls sink independently, so values are emitted and errors sunk in the
// order tap finishes, not the order they were pulled.
func (tl Tail[T]) TapErrorSinkAsync[Tp func(T) error, S func(error)](workers int, tap Tp, sink S) Tail[T] {
	head, tail := New[T](cap(tl))
	go tapErrorSinkAsyncCoordinator(tl, head, workers, tap, sink)
	return tail
}

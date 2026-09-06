package pipe

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// RepeatForever may be passed as the repeat count to [Source], [SourceError], and [SourceErrorSink]
// to call source until the program exits.
const RepeatForever = -1

// Done may be returned by the source passed to [SourceError] and [SourceErrorSink] to end the source
// loop early. Done is never pushed onto a channel or passed to a sink. The source's closer is called
// and the returned channels are closed as if repeat were exhausted.
var Done = errors.New("pipe: source done")

// PanicError is the error emitted on an operation's error path when the function the operation calls
// panics. Value is the recovered panic value.
type PanicError struct {
	Value any
}

// Error returns "pipe: recovered panic: " followed by the formatted [PanicError.Value].
func (e *PanicError) Error() string {
	return fmt.Sprintf("pipe: recovered panic: %v", e.Value)
}

// Unwrap returns [PanicError.Value] when it is an error, otherwise nil.
func (e *PanicError) Unwrap() error {
	if err, ok := e.Value.(error); ok {
		return err
	}
	return nil
}

// recoverFilter calls filter with t. A panic in filter is recovered and returned as a [*PanicError].
func recoverFilter[T any, F func(T) (bool, error)](filter F, t T) (keep bool, err error) {
	defer func() {
		if v := recover(); v != nil {
			err = &PanicError{Value: v}
		}
	}()
	return filter(t)
}

// recoverMap calls mp with t. A panic in mp is recovered and returned as a [*PanicError].
func recoverMap[T any, U any, M func(T) (U, error)](mp M, t T) (u U, err error) {
	defer func() {
		if v := recover(); v != nil {
			err = &PanicError{Value: v}
		}
	}()
	return mp(t)
}

// recoverErr calls fn with t. A panic in fn is recovered and returned as a [*PanicError].
func recoverErr[T any, F func(T) error](fn F, t T) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = &PanicError{Value: v}
		}
	}()
	return fn(t)
}

// recoverSource calls source. A panic in source is recovered and returned as a [*PanicError].
func recoverSource[T any, S func() (T, error)](source S) (t T, err error) {
	defer func() {
		if v := recover(); v != nil {
			err = &PanicError{Value: v}
		}
	}()
	return source()
}

// recoverCloser calls closer. A panic in closer is recovered and returned as a [*PanicError].
func recoverCloser[C func()](closer C) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = &PanicError{Value: v}
		}
	}()
	closer()
	return nil
}

// recoverableSink calls sink with err. A panic in sink is recovered and sink is called again with the
// [*PanicError]. A panic in that second call is not recovered.
func recoverableSink[S func(error)](sink S, err error) {
	defer func() {
		if v := recover(); v != nil {
			sink(&PanicError{Value: v})
		}
	}()
	sink(err)
}

// fanInCoordinator creates len(tails)-1 fanInWorkers then demotes itself to a fanInWorker. Passing a
// nil head only channel or fewer than 1 tail only channel panics. This closes the passed head only
// channel after the last worker exits.
func fanInCoordinator[T any](head Head[T], tails []Tail[T]) {
	defer close(head)

	wg := &sync.WaitGroup{}
	wg.Add(len(tails))
	// skipping the first create a worker for each passed tail only channel
	for _, tail := range tails[1:] {
		go fanInWorker(head, tail, wg)
	}

	// demote to a worker to guarantee there is always one worker running and launch one less
	// goroutine
	fanInWorker(head, tails[0], wg)

	wg.Wait()
}

// fanInWorker iterates over the passed tail only channel forwarding values to the passed head only
// channel. Worker will exit after the pull only channel is closed and emptied.
func fanInWorker[T any](head Head[T], tail Tail[T], wg *sync.WaitGroup) {
	defer wg.Done()

	for t := range tail {
		head <- t
	}
}

// fanOutWorker iterates over the passed tail only channel forwarding each value onto every passed
// head only channel. This closes every passed head only channel after the tail only channel is
// closed and emptied.
func fanOutWorker[T any](tail Tail[T], fan ...Head[T]) {
	defer func() {
		for _, head := range fan {
			close(head)
		}
	}()

	for t := range tail {
		for _, head := range fan {
			head <- t
		}
	}
}

// filterWorker iterates over the passed tail only channel forwarding each value for which filter
// returns true onto the passed head only channel. This closes the passed head only channel after the
// tail only channel is closed and emptied.
func filterWorker[T any, F func(T) bool](tail Tail[T], head Head[T], filter F) {
	defer close(head)

	for t := range tail {
		if filter(t) {
			head <- t
		}
	}
}

// filterAsyncCoordinator creates workers-1 filterAsyncWorkers then demotes itself to a
// filterAsyncWorker. Fewer than 1 workers is treated as 1. This closes the passed head only channel
// after the last worker exits.
func filterAsyncCoordinator[T any, F func(T) bool](tail Tail[T], head Head[T], workers int, filter F) {
	defer close(head)

	if workers < 1 {
		workers = 1
	}

	wg := &sync.WaitGroup{}
	wg.Add(workers)
	for ; workers > 1; workers-- {
		go filterAsyncWorker(tail, head, wg, filter)
	}

	filterAsyncWorker(tail, head, wg, filter)

	wg.Wait()
}

// filterAsyncWorker behaves as filterWorker but shares the passed channels with other workers and
// signals the passed wait group on exit instead of closing the head only channel.
func filterAsyncWorker[T any, F func(T) bool](tail Tail[T], head Head[T], wg *sync.WaitGroup, filter F) {
	defer wg.Done()

	for t := range tail {
		if filter(t) {
			head <- t
		}
	}
}

// filterErrorWorker behaves as filterWorker but filter may return an error. Each error is pushed onto
// the passed error head only channel and the value is discarded. A panic in filter is recovered and
// pushed onto the error head only channel as a [*PanicError]. This closes both passed head only
// channels after the tail only channel is closed and emptied.
func filterErrorWorker[T any, F func(T) (bool, error)](tail Tail[T], head Head[T], err Head[error], filter F) {
	defer func() { close(head); close(err) }()

	for t := range tail {
		if keep, er := recoverFilter(filter, t); er != nil {
			err <- er
		} else if keep {
			head <- t
		}
	}
}

// filterErrorAsyncCoordinator creates workers-1 filterErrorAsyncWorkers then demotes itself to a
// filterErrorAsyncWorker. Fewer than 1 workers is treated as 1. This closes both passed head only
// channels after the last worker exits.
func filterErrorAsyncCoordinator[T any, F func(T) (bool, error)](tail Tail[T], head Head[T], err Head[error], workers int, filter F) {
	defer func() { close(head); close(err) }()

	if workers < 1 {
		workers = 1
	}

	wg := &sync.WaitGroup{}
	wg.Add(workers)
	for ; workers > 1; workers-- {
		go filterErrorAsyncWorker(tail, head, err, wg, filter)
	}

	filterErrorAsyncWorker(tail, head, err, wg, filter)

	wg.Wait()
}

// filterErrorAsyncWorker behaves as filterErrorWorker but shares the passed channels with other
// workers and signals the passed wait group on exit instead of closing the head only channels.
func filterErrorAsyncWorker[T any, F func(T) (bool, error)](tail Tail[T], head Head[T], err Head[error], wg *sync.WaitGroup, filter F) {
	defer wg.Done()

	for t := range tail {
		if keep, er := recoverFilter(filter, t); er != nil {
			err <- er
		} else if keep {
			head <- t
		}
	}
}

// filterErrorSinkWorker behaves as filterErrorWorker but each error is passed to sink instead of
// being pushed onto a channel.
//
// Panics:
//   - a panic in filter is recovered and passed to sink as a [*PanicError]
//   - a panic in sink is recovered and sink is called again with the [*PanicError]
//   - a panic in that second call is not recovered
func filterErrorSinkWorker[T any, F func(T) (bool, error), S func(error)](tail Tail[T], head Head[T], filter F, sink S) {
	defer close(head)

	for t := range tail {
		if keep, er := recoverFilter(filter, t); er != nil {
			recoverableSink(sink, er)
		} else if keep {
			head <- t
		}
	}
}

// filterErrorSinkAsyncCoordinator creates workers-1 filterErrorSinkAsyncWorkers then demotes itself
// to a filterErrorSinkAsyncWorker. Fewer than 1 workers is treated as 1. This closes the passed head
// only channel after the last worker exits.
func filterErrorSinkAsyncCoordinator[T any, F func(T) (bool, error), S func(error)](tail Tail[T], head Head[T], workers int, filter F, sink S) {
	defer close(head)

	if workers < 1 {
		workers = 1
	}

	wg := &sync.WaitGroup{}
	wg.Add(workers)
	for ; workers > 1; workers-- {
		go filterErrorSinkAsyncWorker(tail, head, wg, filter, sink)
	}

	filterErrorSinkAsyncWorker(tail, head, wg, filter, sink)

	wg.Wait()
}

// filterErrorSinkAsyncWorker behaves as filterErrorSinkWorker but shares the passed channels with
// other workers and signals the passed wait group on exit instead of closing the head only channel.
func filterErrorSinkAsyncWorker[T any, F func(T) (bool, error), S func(error)](tail Tail[T], head Head[T], wg *sync.WaitGroup, filter F, sink S) {
	defer wg.Done()

	for t := range tail {
		if keep, er := recoverFilter(filter, t); er != nil {
			recoverableSink(sink, er)
		} else if keep {
			head <- t
		}
	}
}

// mapWorker iterates over the passed tail only channel pushing the result of mp for each value onto
// the passed head only channel. This closes the passed head only channel after the tail only channel
// is closed and emptied.
func mapWorker[T any, U any, M func(T) U](tail Tail[T], head Head[U], mp M) {
	defer close(head)

	for t := range tail {
		head <- mp(t)
	}
}

// mapAsyncCoordinator creates workers-1 mapAsyncWorkers then demotes itself to a mapAsyncWorker.
// Fewer than 1 workers is treated as 1. This closes the passed head only channel after the last
// worker exits.
func mapAsyncCoordinator[T any, U any, M func(T) U](tail Tail[T], head Head[U], workers int, mp M) {
	defer close(head)

	if workers < 1 {
		workers = 1
	}

	wg := &sync.WaitGroup{}
	wg.Add(workers)
	for ; workers > 1; workers-- {
		go mapAsyncWorker(tail, head, wg, mp)
	}

	// demote to a worker to guarantee there is always one worker running and launch one less
	// goroutine
	mapAsyncWorker(tail, head, wg, mp)

	wg.Wait()
}

// mapAsyncWorker behaves as mapWorker but shares the passed channels with other workers and signals
// the passed wait group on exit instead of closing the head only channel.
func mapAsyncWorker[T any, U any, M func(T) U](tail Tail[T], head Head[U], wg *sync.WaitGroup, mp M) {
	defer wg.Done()

	for t := range tail {
		head <- mp(t)
	}
}

// mapErrorWorker behaves as mapWorker but mp may return an error. Each error is pushed onto the
// passed error head only channel and no value is pushed for that input. A panic in mp is recovered
// and pushed onto the error head only channel as a [*PanicError]. This closes both passed head only
// channels after the tail only channel is closed and emptied.
func mapErrorWorker[T any, U any, M func(T) (U, error)](tail Tail[T], head Head[U], err Head[error], mp M) {
	defer func() { close(head); close(err) }()

	for t := range tail {
		if n, er := recoverMap(mp, t); er != nil {
			err <- er
		} else {
			head <- n
		}
	}
}

// mapErrorAsyncCoordinator creates workers-1 mapErrorAsyncWorkers then demotes itself to a
// mapErrorAsyncWorker. Fewer than 1 workers is treated as 1. This closes both passed head only
// channels after the last worker exits.
func mapErrorAsyncCoordinator[T any, U any, M func(T) (U, error)](tail Tail[T], head Head[U], err Head[error], workers int, mp M) {
	defer func() { close(head); close(err) }()

	if workers < 1 {
		workers = 1
	}

	wg := &sync.WaitGroup{}
	wg.Add(workers)
	for ; workers > 1; workers-- {
		go mapErrorAsyncWorker(tail, head, err, wg, mp)
	}

	// demote to a worker to guarantee there is always one worker running and launch one less
	// goroutine
	mapErrorAsyncWorker(tail, head, err, wg, mp)

	wg.Wait()
}

// mapErrorAsyncWorker behaves as mapErrorWorker but shares the passed channels with other workers and
// signals the passed wait group on exit instead of closing the head only channels.
func mapErrorAsyncWorker[T any, U any, M func(T) (U, error)](tail Tail[T], head Head[U], err Head[error], wg *sync.WaitGroup, mp M) {
	defer wg.Done()

	for t := range tail {
		if n, er := recoverMap(mp, t); er != nil {
			err <- er
		} else {
			head <- n
		}
	}
}

// mapErrorSinkWorker behaves as mapErrorWorker but each error is passed to sink instead of being
// pushed onto a channel.
//
// Panics:
//   - a panic in mp is recovered and passed to sink as a [*PanicError]
//   - a panic in sink is recovered and sink is called again with the [*PanicError]
//   - a panic in that second call is not recovered
func mapErrorSinkWorker[T any, U any, M func(T) (U, error), S func(error)](tail Tail[T], head Head[U], mp M, sink S) {
	defer close(head)

	for t := range tail {
		if n, er := recoverMap(mp, t); er != nil {
			recoverableSink(sink, er)
		} else {
			head <- n
		}
	}
}

// mapErrorSinkAsyncCoordinator creates workers-1 mapErrorSinkAsyncWorkers then demotes itself to a
// mapErrorSinkAsyncWorker. Fewer than 1 workers is treated as 1. This closes the passed head only
// channel after the last worker exits.
func mapErrorSinkAsyncCoordinator[T any, U any, M func(T) (U, error), S func(error)](tail Tail[T], head Head[U], workers int, mp M, sink S) {
	defer close(head)
	if workers < 1 {
		workers = 1
	}

	wg := &sync.WaitGroup{}
	wg.Add(workers)
	for ; workers > 1; workers-- {
		go mapErrorSinkAsyncWorker(tail, head, wg, mp, sink)
	}

	// demote to a worker to guarantee there is always one worker running and launch only `workers`
	// goroutines
	mapErrorSinkAsyncWorker(tail, head, wg, mp, sink)

	wg.Wait()
}

// mapErrorSinkAsyncWorker behaves as mapErrorSinkWorker but shares the passed channels with other
// workers and signals the passed wait group on exit instead of closing the head only channel.
func mapErrorSinkAsyncWorker[T any, U any, M func(T) (U, error), S func(error)](tail Tail[T], head Head[U], wg *sync.WaitGroup, mp M, sink S) {
	defer wg.Done()
	for t := range tail {
		if n, er := recoverMap(mp, t); er != nil {
			recoverableSink(sink, er)
		} else {
			head <- n
		}
	}
}

// reduceAndEmitWorker iterates over the passed tail only channel calling reduce with each value and
// the current accumulator, starting from acc. Once the tail only channel is closed and emptied the
// final accumulator is pushed onto the passed head only channel and the channel is closed.
func reduceAndEmitWorker[T any, Acc any, R func(T, Acc) Acc](tail Tail[T], head Head[Acc], reduce R, acc Acc) {
	defer close(head)

	for t := range tail {
		acc = reduce(t, acc)
	}

	head <- acc
}

// windowWorker iterates over the passed tail only channel calling reduce with each value and the
// current accumulator. Every window duration the accumulator is pushed onto the passed head only
// channel and replaced with a fresh one from acc. Once the tail only channel is closed and emptied
// the final accumulator is pushed and the head only channel is closed.
func windowWorker[T any, Acc any, A func() Acc, R func(T, Acc) Acc](tail Tail[T], head Head[Acc], window time.Duration, reduce R, acc A) {
	defer close(head)

	ticker := time.NewTicker(window)
	defer ticker.Stop()

	ac := acc()
	for {
		select {
		case t, ok := <-tail:
			if !ok {
				head <- ac
				return
			}
			ac = reduce(t, ac)

		case <-ticker.C:
			head <- ac
			ac = acc()
		}
	}
}

// routerWorker iterates over the passed tail only channel forwarding each value onto the route whose
// key equals compare(value). A value matching no route is forwarded onto orElse. This closes every
// route in the map and orElse after the tail only channel is closed and emptied.
func routerWorker[T any, Cmp comparable, C func(T) Cmp](tail Tail[T], routes map[Cmp]Head[T], orElse Head[T], compare C) {
	defer func() {
		for _, head := range routes {
			close(head)
		}

		close(orElse)
	}()

	for t := range tail {
		if route, exists := routes[compare(t)]; exists {
			route <- t
			continue
		}

		orElse <- t
	}
}

// routerAsyncCoordinator creates workers-1 routerAsyncWorkers then demotes itself to a
// routerAsyncWorker. Fewer than 1 workers is treated as 1. This closes every route in the map and
// orElse after the last worker exits.
func routerAsyncCoordinator[T any, Cmp comparable, C func(T) Cmp](tail Tail[T], routes map[Cmp]Head[T], orElse Head[T], workers int, compare C) {
	defer func() {
		for _, head := range routes {
			close(head)
		}

		close(orElse)
	}()

	if workers < 1 {
		workers = 1
	}

	wg := &sync.WaitGroup{}
	wg.Add(workers)
	for ; workers > 1; workers-- {
		go routerAsyncWorker(tail, routes, orElse, wg, compare)
	}

	routerAsyncWorker(tail, routes, orElse, wg, compare)

	wg.Wait()
}

// routerAsyncWorker behaves as routerWorker but shares the passed channels with other workers and
// signals the passed wait group on exit instead of closing the head only channels.
func routerAsyncWorker[T any, Cmp comparable, C func(T) Cmp](tail Tail[T], routes map[Cmp]Head[T], orElse Head[T], wg *sync.WaitGroup, compare C) {
	defer wg.Done()

	for t := range tail {
		if route, exists := routes[compare(t)]; exists {
			route <- t
			continue
		}

		orElse <- t
	}
}

// routerWithSinkWorker behaves as routerWorker but a value matching no route is passed to sink
// instead of being forwarded onto a channel.
func routerWithSinkWorker[T any, Cmp comparable, C func(T) Cmp, S func(T)](tail Tail[T], routes map[Cmp]Head[T], compare C, sink S) {
	defer func() {
		for _, head := range routes {
			close(head)
		}
	}()

	for t := range tail {
		if route, exists := routes[compare(t)]; exists {
			route <- t
			continue
		}

		sink(t)
	}
}

// routerWithSinkAsyncCoordinator creates workers-1 routerWithSinkAsyncWorkers then demotes itself to
// a routerWithSinkAsyncWorker. Fewer than 1 workers is treated as 1. This closes every route in the
// map after the last worker exits.
func routerWithSinkAsyncCoordinator[T any, Cmp comparable, C func(T) Cmp, S func(T)](tail Tail[T], routes map[Cmp]Head[T], workers int, compare C, sink S) {
	defer func() {
		for _, head := range routes {
			close(head)
		}
	}()

	if workers < 1 {
		workers = 1
	}

	wg := &sync.WaitGroup{}
	wg.Add(workers)
	for ; workers > 1; workers-- {
		go routerWithSinkAsyncWorker(tail, routes, wg, compare, sink)
	}

	routerWithSinkAsyncWorker(tail, routes, wg, compare, sink)

	wg.Wait()
}

// routerWithSinkAsyncWorker behaves as routerWithSinkWorker but shares the passed channels with other
// workers and signals the passed wait group on exit instead of closing the head only channels.
func routerWithSinkAsyncWorker[T any, Cmp comparable, C func(T) Cmp, S func(T)](tail Tail[T], routes map[Cmp]Head[T], wg *sync.WaitGroup, compare C, sink S) {
	defer wg.Done()

	for t := range tail {
		if route, exists := routes[compare(t)]; exists {
			route <- t
			continue
		}

		sink(t)
	}
}

// roundRobinChooser returns a chooser yielding each index from 0 to count-1 in turn, wrapping back to
// 0 after count-1. The returned chooser is not safe for concurrent use.
func roundRobinChooser[T any](count int) func(T) int {
	lastIdx := 0
	return func(t T) int {
		if lastIdx >= count {
			lastIdx = 0
		}

		idx := lastIdx
		lastIdx++

		return idx
	}
}

// distrbuteWorker iterates over the passed tail only channel forwarding each value onto the head only
// channel at index choose(value). A choose result outside the bounds of heads panics. This closes
// every passed head only channel after the tail only channel is closed and emptied.
func distrbuteWorker[T any, C func(T) int](tail Tail[T], heads []Head[T], choose C) {
	defer func() {
		for _, head := range heads {
			close(head)
		}
	}()

	for t := range tail {
		heads[choose(t)] <- t
	}
}

// distributeAsyncCoordinator creates workers-1 distributeAsyncWorkers then demotes itself to a
// distributeAsyncWorker. Fewer than 1 workers is treated as 1. choose is called from every worker
// and must be safe for concurrent use. This closes every passed head only channel after the last
// worker exits.
func distributeAsyncCoordinator[T any, C func(T) int](tail Tail[T], heads []Head[T], workers int, choose C) {
	defer func() {
		for _, head := range heads {
			close(head)
		}
	}()

	if workers < 1 {
		workers = 1
	}

	wg := &sync.WaitGroup{}
	wg.Add(workers)
	for ; workers > 1; workers-- {
		go distributeAsyncWorker(tail, heads, wg, choose)
	}

	distributeAsyncWorker(tail, heads, wg, choose)

	wg.Wait()
}

// distributeAsyncWorker behaves as distrbuteWorker but shares the passed channels with other workers
// and signals the passed wait group on exit instead of closing the head only channels.
func distributeAsyncWorker[T any, C func(T) int](tail Tail[T], heads []Head[T], wg *sync.WaitGroup, choose C) {
	defer wg.Done()

	for t := range tail {
		heads[choose(t)] <- t
	}
}

// tapWorker iterates over the passed tail only channel passing each value to tap and then forwarding
// it onto the passed head only channel. This closes the passed head only channel after the tail only
// channel is closed and emptied.
func tapWorker[T any, Tp func(T)](tail Tail[T], head Head[T], tap Tp) {
	defer close(head)
	for t := range tail {
		tap(t)
		head <- t
	}
}

// tapAsyncCoordinator creates workers-1 tapAsyncWorkers then demotes itself to a tapAsyncWorker.
// Fewer than 1 workers is treated as 1. This closes the passed head only channel after the last
// worker exits.
func tapAsyncCoordinator[T any, Tp func(T)](tail Tail[T], head Head[T], workers int, tap Tp) {
	defer close(head)

	if workers < 1 {
		workers = 1
	}

	wg := &sync.WaitGroup{}
	wg.Add(workers)
	for ; workers > 1; workers-- {
		go tapAsyncWorker(tail, head, wg, tap)
	}

	tapAsyncWorker(tail, head, wg, tap)

	wg.Wait()
}

// tapAsyncWorker behaves as tapWorker but shares the passed channels with other workers and signals
// the passed wait group on exit instead of closing the head only channel.
func tapAsyncWorker[T any, Tp func(T)](tail Tail[T], head Head[T], wg *sync.WaitGroup, tap Tp) {
	defer wg.Done()
	for t := range tail {
		tap(t)
		head <- t
	}
}

// tapErrorWorker behaves as tapWorker but tap may return an error. Each error is pushed onto the
// passed error head only channel and the value is still forwarded. A panic in tap is recovered and
// pushed onto the error head only channel as a [*PanicError]. This closes both passed head only
// channels after the tail only channel is closed and emptied.
func tapErrorWorker[T any, Tp func(T) error](tail Tail[T], head Head[T], err Head[error], tap Tp) {
	defer func() { close(head); close(err) }()
	for t := range tail {
		if er := recoverErr(tap, t); er != nil {
			err <- er
		}
		head <- t
	}
}

// tapErrorAsyncCoordinator creates workers-1 tapErrorAsyncWorkers then demotes itself to a
// tapErrorAsyncWorker. Fewer than 1 workers is treated as 1. This closes both passed head only
// channels after the last worker exits.
func tapErrorAsyncCoordinator[T any, Tp func(T) error](tail Tail[T], head Head[T], err Head[error], workers int, tap Tp) {
	defer func() { close(head); close(err) }()

	if workers < 1 {
		workers = 1
	}

	wg := &sync.WaitGroup{}
	wg.Add(workers)
	for ; workers > 1; workers-- {
		go tapErrorAsyncWorker(tail, head, err, wg, tap)
	}

	tapErrorAsyncWorker(tail, head, err, wg, tap)

	wg.Wait()
}

// tapErrorAsyncWorker behaves as tapErrorWorker but shares the passed channels with other workers and
// signals the passed wait group on exit instead of closing the head only channels.
func tapErrorAsyncWorker[T any, Tp func(T) error](tail Tail[T], head Head[T], err Head[error], wg *sync.WaitGroup, tap Tp) {
	defer wg.Done()
	for t := range tail {
		if er := recoverErr(tap, t); er != nil {
			err <- er
		}
		head <- t
	}
}

// tapErrorSinkWorker behaves as tapErrorWorker but each error is passed to sink instead of being
// pushed onto a channel.
//
// Panics:
//   - a panic in tap is recovered and passed to sink as a [*PanicError]
//   - a panic in sink is recovered and sink is called again with the [*PanicError]
//   - a panic in that second call is not recovered
func tapErrorSinkWorker[T any, Tp func(T) error, S func(error)](tail Tail[T], head Head[T], tap Tp, sink S) {
	defer close(head)
	for t := range tail {
		if er := recoverErr(tap, t); er != nil {
			recoverableSink(sink, er)
		}
		head <- t
	}
}

// tapErrorSinkAsyncCoordinator creates workers-1 tapErrorSinkAsyncWorkers then demotes itself to a
// tapErrorSinkAsyncWorker. Fewer than 1 workers is treated as 1. This closes the passed head only
// channel after the last worker exits.
func tapErrorSinkAsyncCoordinator[T any, Tp func(T) error, S func(error)](tail Tail[T], head Head[T], workers int, tap Tp, sink S) {
	defer close(head)

	if workers < 1 {
		workers = 1
	}

	wg := &sync.WaitGroup{}
	wg.Add(workers)
	for ; workers > 1; workers-- {
		go tapErrorSinkAsyncWorker(tail, head, wg, tap, sink)
	}

	tapErrorSinkAsyncWorker(tail, head, wg, tap, sink)

	wg.Wait()
}

// tapErrorSinkAsyncWorker behaves as tapErrorSinkWorker but shares the passed channels with other
// workers and signals the passed wait group on exit instead of closing the head only channel.
func tapErrorSinkAsyncWorker[T any, Tp func(T) error, S func(error)](tail Tail[T], head Head[T], wg *sync.WaitGroup, tap Tp, sink S) {
	defer wg.Done()
	for t := range tail {
		if er := recoverErr(tap, t); er != nil {
			recoverableSink(sink, er)
		}
		head <- t
	}
}

// sinkAsyncCoordinator creates workers-1 sinkAsyncWorkers then demotes itself to a sinkAsyncWorker.
// Fewer than 1 workers is treated as 1. This exits after the last worker exits.
func sinkAsyncCoordinator[T any, S func(T)](tail Tail[T], workers int, sink S) {
	if workers < 1 {
		workers = 1
	}

	wg := &sync.WaitGroup{}
	wg.Add(workers)
	for ; workers > 1; workers-- {
		go sinkAsyncWorker(tail, wg, sink)
	}

	sinkAsyncWorker(tail, wg, sink)

	wg.Wait()
}

// sinkAsyncWorker iterates over the passed tail only channel passing each value to sink. The tail
// only channel is shared with other workers. This signals the passed wait group on exit.
func sinkAsyncWorker[T any, S func(T)](tail Tail[T], wg *sync.WaitGroup, sink S) {
	defer wg.Done()
	for t := range tail {
		sink(t)
	}
}

// sinkErrorWorker iterates over the passed tail only channel passing each value to sink. Each error
// returned by sink is pushed onto the passed error head only channel. A panic in sink is recovered
// and pushed onto the error head only channel as a [*PanicError]. This closes the passed error head
// only channel after the tail only channel is closed and emptied.
func sinkErrorWorker[T any, S func(T) error](tail Tail[T], err Head[error], sink S) {
	defer close(err)
	for t := range tail {
		if er := recoverErr(sink, t); er != nil {
			err <- er
		}
	}
}

// sinkErrorAsyncCoordinator creates workers-1 sinkErrorAsyncWorkers then demotes itself to a
// sinkErrorAsyncWorker. Fewer than 1 workers is treated as 1. This closes the passed error head
// only channel after the last worker exits.
func sinkErrorAsyncCoordinator[T any, S func(T) error](tail Tail[T], err Head[error], workers int, sink S) {
	defer close(err)

	if workers < 1 {
		workers = 1
	}

	wg := &sync.WaitGroup{}
	wg.Add(workers)
	for ; workers > 1; workers-- {
		go sinkErrorAsyncWorker(tail, err, wg, sink)
	}

	sinkErrorAsyncWorker(tail, err, wg, sink)

	wg.Wait()
}

// sinkErrorAsyncWorker behaves as sinkErrorWorker but shares the passed channels with other workers
// and signals the passed wait group on exit instead of closing the error head only channel.
func sinkErrorAsyncWorker[T any, S func(T) error](tail Tail[T], err Head[error], wg *sync.WaitGroup, sink S) {
	defer wg.Done()
	for t := range tail {
		if er := recoverErr(sink, t); er != nil {
			err <- er
		}
	}
}

// sinkErrorSinkAsyncCoordinator creates workers-1 sinkErrorSinkAsyncWorkers then demotes itself to
// a sinkErrorSinkAsyncWorker. Fewer than 1 workers is treated as 1. This exits after the last
// worker exits.
func sinkErrorSinkAsyncCoordinator[T any, S func(T) error, E func(error)](tail Tail[T], workers int, sink S, errSink E) {
	if workers < 1 {
		workers = 1
	}

	wg := &sync.WaitGroup{}
	wg.Add(workers)
	for ; workers > 1; workers-- {
		go sinkErrorSinkAsyncWorker(tail, wg, sink, errSink)
	}

	sinkErrorSinkAsyncWorker(tail, wg, sink, errSink)

	wg.Wait()
}

// sinkErrorSinkAsyncWorker behaves as sinkAsyncWorker but sink may return an error. Each error is
// passed to errSink.
//
// Panics:
//   - a panic in sink is recovered and passed to errSink as a [*PanicError]
//   - a panic in errSink is recovered and errSink is called again with the [*PanicError]
//   - a panic in that second call is not recovered
func sinkErrorSinkAsyncWorker[T any, S func(T) error, E func(error)](tail Tail[T], wg *sync.WaitGroup, sink S, errSink E) {
	defer wg.Done()
	for t := range tail {
		if er := recoverErr(sink, t); er != nil {
			recoverableSink(errSink, er)
		}
	}
}

// sourceWorker calls source repeat times pushing each result onto the passed head only channel.
// Passing [RepeatForever] calls source until the program exits. Passing 0 or any other negative value
// calls source zero times. Once repeat is exhausted this calls closer and then closes the passed head
// only channel.
func sourceWorker[T any, S func() T, C func()](head Head[T], repeat int, source S, closer C) {
	defer close(head)
	defer closer()
	switch repeat {
	case RepeatForever:
		for {
			head <- source()
		}
	default:
		for ; repeat > 0; repeat-- {
			head <- source()
		}
	}
}

// sourceErrorWorker behaves as sourceWorker but source may return an error. Each error is pushed onto
// the passed error head only channel and no value is pushed for that call. Returning [Done] from
// source ends the loop as if repeat were exhausted and [Done] is not pushed. This then calls closer
// and closes both passed head only channels.
//
// Panics:
//   - a panic in source is recovered and pushed onto the error head only channel as a [*PanicError]
//   - a panic in closer is recovered and pushed onto the error head only channel as a [*PanicError]
func sourceErrorWorker[T any, S func() (T, error), C func()](head Head[T], err Head[error], repeat int, source S, closer C) {
	defer func() {
		if er := recoverCloser(closer); er != nil {
			err <- er
		}
		close(err)
		close(head)
	}()

	switch repeat {
	case RepeatForever:
		for {
			if v, er := recoverSource(source); errors.Is(er, Done) {
				return
			} else if er != nil {
				err <- er
			} else {
				head <- v
			}
		}
	default:
		for ; repeat > 0; repeat-- {
			if v, er := recoverSource(source); errors.Is(er, Done) {
				return
			} else if er != nil {
				err <- er
			} else {
				head <- v
			}
		}
	}
}

// sourceErrorSinkWorker behaves as sourceErrorWorker but each error is passed to sink instead of
// being pushed onto a channel. [Done] is not passed to sink.
//
// Panics:
//   - a panic in source is recovered and passed to sink as a [*PanicError]
//   - a panic in closer is recovered and passed to sink as a [*PanicError]
//   - a panic in sink is recovered and sink is called again with the [*PanicError]
//   - a panic in that second call is not recovered
func sourceErrorSinkWorker[T any, S func() (T, error), C func(), E func(error)](head Head[T], repeat int, source S, closer C, sink E) {
	defer func() {
		if err := recoverCloser(closer); err != nil {
			recoverableSink(sink, err)
		}
		close(head)
	}()

	switch repeat {
	case RepeatForever:
		for {
			if v, err := recoverSource(source); errors.Is(err, Done) {
				return
			} else if err != nil {
				recoverableSink(sink, err)
			} else {
				head <- v
			}
		}
	default:
		for ; repeat > 0; repeat-- {
			if v, err := recoverSource(source); errors.Is(err, Done) {
				return
			} else if err != nil {
				recoverableSink(sink, err)
			} else {
				head <- v
			}
		}
	}
}

# Pipe - Type Safe Functional Channels

This library melds functional programming paradigms and common Go function call patterns over top of Go's channels in a `type safe` manner. A channel is split into its `Head[T]` (push only) and `Tail[T]` (pull only) ends. `Tail[T]` carries the functional operations such as `Filter`, `Map`, and `Reduce`, with `Error` and `ErrorSink` variants for functions that can fail and `Async` variants such as `MapAsync` that run a function across a pool of workers at the cost of ordering. `Source` builds a `Tail[T]` from a function and calls a closer when the function is exhausted or returns `Done`. Every operation manages the lifecycle of the channels and goroutines it creates, so the library user never closes a channel they did not make.

Requires Go `1.27` for parameterized methods.

## Dependencies

This library imports only the standard library and has no 3rd party dependencies. The goal is to maintain this always for produciton code. The exception being importing 3rd party libraries for testing purposes only.

# Usage

```go
head, tail := pipe.New[int](16)

go func() {
    defer head.Close()
    for i := 0; i < 100; i++ {
        head.Push(i)
    }
}()

sum := tail.
    Filter(func(i int) bool { return i%2 == 0 }).
    Map(func(i int) int { return i * i }).
    Reduce(0, func(i, acc int) int { return acc + i })
```

Each `Tail[T]` method returns a new `Tail[T]` (or a value for the blocking operations `Reduce`, `Sink`, `Drain`, `Wait`) and closes what it returns after its input is closed and emptied. Chains read top to bottom in the order values flow.

# Examples

The `example` folder contains functional, if sometimes contrived, example implementations of library functionality. `example/dir_scan` walks a directory, hashes every file across a pool of goroutines, and reports throughput in one-second windows.


// Package pipe layers functional operations over Go channels in a type safe manner. A channel is
// split into its [Head] (push only) and [Tail] (pull only) ends. [Tail] carries the operations such
// as [Tail.Filter], [Tail.Map], [Tail.Reduce], [Tail.Router], and [Tail.Sink], each returning a new
// [Tail] or a final value. Every operation creates and closes the channels and goroutines it needs,
// so a user of the package never closes a channel they did not make.
//
// # Errors
//
// Operations whose function may fail come in Error and ErrorSink forms. The Error form returns a
// second [Tail] carrying every error. The ErrorSink form passes every error to a sink function
// instead.
//
// # Sources
//
// [Source], [SourceError], and [SourceErrorSink] build a [Tail] from a function that produces
// values, call a closer when the source is finished, and stop early when the source returns [Done].
//
// # Async
//
// Operations with an Async form run their function across a number of worker goroutines pulling
// from the same channel. With more than one worker, a slow call to the function holds up only its
// own worker while the others keep pulling, so one operation does not block the rest. The trade off
// is ordering. Workers push as they finish, so values leave an Async operation in the order the
// function completes rather than the order they arrived. Nothing downstream restores that order, so
// a chain that depends on arrival order either avoids the Async forms or reorders the values itself.
//
// # Bad inputs
//
// An operation given an input it cannot use panics at the call, in the caller's goroutine, before
// any channel or worker is created. The panic value is a string of the form
// "pipe: <Method>: <problem>", for example "pipe: Tail.Filter: nil filter". Bad inputs are:
//
//   - a nil [Tail] receiver, or a nil [Head] receiver on [Head.Push]
//   - a nil [Tail] passed to [FanIn]
//   - a nil function argument
//   - a window of zero or less on [Tail.Window]
//
// [Tail.TryPull] and [Head.TryPush] return false on a nil receiver instead of panicking. A nil
// closer passed to [Source], [SourceError], or [SourceErrorSink] is treated as a no-op.
package pipe

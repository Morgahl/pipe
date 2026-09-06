package pipe

// Head is the push only end of a channel. This is essentially a chan<- T and can be used the same
// way as one would use a send only channel in Go under normal syntax usages. However this variant
// has methods for pushing onto and closing the channel. A T can only be pushed onto a Head.
type Head[T any] chan<- T

// Close closes the channel. Any attempts to push to a closed channel will panic. Closing a closed or
// nil Head panics.
func (hd Head[T]) Close() {
	close(hd)
}

// Push is a blocking operation that pushes a T onto the channel. This blocks while the channel is
// full. This will panic if the channel is closed. Pushing onto a nil Head panics.
func (hd Head[T]) Push(t T) {
	assert(hd != nil, "pipe: Head.Push: nil %T", hd)
	hd <- t
}

// TryPush is a non-blocking operation that attempts to push a T onto the channel. This returns true
// if the T was successfully pushed, false if the channel was blocked or nil. It is exceedingly
// unlikely that you will ever successfully push onto an unbuffered channel as this requires the
// scheduler to have a recieveing goroutine ready. A closed Head panics.
func (hd Head[T]) TryPush(t T) (ok bool) {
	select {
	case hd <- t:
		return true
	default:
		return false
	}
}

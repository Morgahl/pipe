package pipe

import "fmt"

// assert panics with the formatted message when ok is false. Exported functions and methods validate
// their inputs through assert before creating any channel or goroutine, so a bad input panics in the
// caller's goroutine at the call site.
func assert(ok bool, format string, args ...any) {
	if !ok {
		panic(fmt.Sprintf(format, args...))
	}
}

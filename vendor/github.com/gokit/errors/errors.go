package errors

import (
	"bytes"
	"fmt"
	"runtime"
	"time"
)

// vars
const (
	stackSize   = 1 << 13
	unknownName = "Unknown()"
	unknownFile = "???"
)

// IsAny returns true/false any of giving error matches set of error.
func IsAny(err error, set ...error) bool {
	err = UnwrapDeep(err)
	for _, item := range set {
		if item == err {
			return true
		}
	}
	return false
}

// ErrorOption defines a giving function which receiving
// a giving error returns another modified with respect
// to it's internal intent.
type ErrorOption func(error) error

// Apply applies giving set of ErrorOptions to provided error.
func Apply(err error, ops ...ErrorOption) error {
	for _, op := range ops {
		err = op(err)
	}
	return err
}

// StackWith will attempt to add call stack into provided error if
// error is a PointingError type without a stack.
func StackWith(st *StackTrace) ErrorOption {
	return func(e error) error {
		pe := unwrapAs(e)
		if pe.Stack == nil {
			pe.Stack = st
		}
		return pe
	}
}

// Meta adds giving header map as meta information to error.
func Meta(err error, header map[string]interface{}) ErrorOption {
	return func(e error) error {
		pe := unwrapAs(e)
		pe.Meta = header
		return pe
	}
}

// Stacked returns an error from provided message and parameter
// list if provided. It adds necessary information related
// to point of return.
func Stacked() ErrorOption {
	return func(e error) error {
		next := unwrapAs(e)
		next.Stack = Stack(1)
		next.Call = GetMethodGraph(3, 9)
		return next
	}
}

// StackedBy returns an error from provided message and parameter
// list if provided. It adds necessary information related
// to point of return.
func StackedBy(n int) ErrorOption {
	return func(e error) error {
		next := unwrapAs(e)
		next.Stack = Stack(n)
		next.Call = GetMethodGraph(3, 9)
		return next
	}
}

// StackWrap returns a new error which wraps existing error value if
// present and also collects current stack trace into returned error.
// It formats message accordingly with arguments from
// variadic list v.
func StackWrap(err error, message string, v ...interface{}) error {
	if len(v) != 0 {
		message = fmt.Sprintf(message, v...)
	}

	var next PointingError
	next.Parent = err
	next.Stack = Stack(0)
	next.Message = message
	next.Call = GetMethodGraph(3, 9)
	return &next
}

// NewStack returns a new error which wraps existing error value if
// present and also collects current stack trace into returned error.
// It formats message accordingly with arguments from
// variadic list v.
func NewStack(message string, v ...interface{}) error {
	if len(v) != 0 {
		message = fmt.Sprintf(message, v...)
	}

	var next PointingError
	next.Stack = Stack(0)
	next.Message = message
	next.Call = GetMethodGraph(3, 9)
	return &next
}

// New returns an error from provided message and parameter
// list if provided. It adds necessary information related
// to point of return.
func New(message string, v ...interface{}) error {
	if len(v) != 0 {
		message = fmt.Sprintf(message, v...)
	}

	var next PointingError
	next.Message = message
	next.Call = GetMethodGraph(3, 9)
	return &next
}

// NewBy returns an error from provided message and parameter
// list if provided. It adds necessary information related
// to point of return.
func NewBy(n int, message string, v ...interface{}) error {
	if len(v) != 0 {
		message = fmt.Sprintf(message, v...)
	}

	var next PointingError
	next.Message = message
	next.Call = GetMethodGraph(3, n)
	return &next
}

// Wrap returns a new error which wraps existing error value if
// present. It formats message accordingly with arguments from
// variadic list v.
func Wrap(err error, message string, v ...interface{}) error {
	if len(v) != 0 {
		message = fmt.Sprintf(message, v...)
	}

	next := wrapOnly(err)
	next.Parent = err
	next.Message = message
	return next
}

// WrapBy returns a new error which wraps existing error value if
// present. It formats message accordingly with arguments from
// variadic list v.
func WrapBy(n int, err error, message string, v ...interface{}) error {
	if len(v) != 0 {
		message = fmt.Sprintf(message, v...)
	}

	var next PointingError
	next.Parent = err
	next.Message = message
	next.Call = GetMethodGraph(3, n)
	return next
}

// WrapOnly returns a new error which wraps existing error value if
// present.
func WrapOnly(err error) error {
	return wrapOnly(err)
}

// Unwrap returns the underline error of giving PointingError.
func Unwrap(e error) error {
	if tm, ok := e.(*PointingError); ok {
		if tm.Parent == nil {
			return tm
		}
		return tm.Parent
	}
	return e
}

// UnwrapDeep returns the root error wrapped by all possible PointingError types.
// It attempts to retrieve the original error.
func UnwrapDeep(e error) error {
	if tm, ok := e.(*PointingError); ok {
		if tm.Parent == nil {
			return tm
		}

		return UnwrapDeep(tm.Parent)
	}

	return e
}

// wrapOnly returns a new error which wraps existing error value if
// present.
func wrapOnly(err error) *PointingError {
	return wrapOnlyBy(err, 4, 9)
}

// WrapOnlyBy returns a new error which wraps existing error value if
// present.
func wrapOnlyBy(err error, depth int, stack int) *PointingError {
	var next PointingError
	next.Parent = err
	next.Message = err.Error()
	next.Call = GetMethodGraph(depth, stack)
	return &next
}

// unwrapAs unwraps giving error to PointingError if it is else wraps
// and returns a new PointingError.
func unwrapAs(e error) *PointingError {
	if tm, ok := e.(*PointingError); ok {
		return tm
	}
	return wrapOnlyBy(e, 4, 9)
}

// PointingError defines a custom error type which points to
// both an originating point of return and a parent error if
// wrapped.
type PointingError struct {
	Message string
	Call    CallGraph
	Stack   *StackTrace
	Meta    map[string]interface{}
	Parent  error
}

// Error implements the error interface.
func (pe PointingError) Error() string {
	return pe.String()
}

// String returns formatted string.
func (pe PointingError) String() string {
	var buf bytes.Buffer
	pe.Format(&buf)
	return buf.String()
}

// Format writes details of error into provided buffer.
func (pe *PointingError) Format(buf *bytes.Buffer) {
	buf.WriteString("[!] Error ")
	buf.WriteString("Message: ")
	if pe.Message == "" && pe.Parent != nil {
		buf.WriteString("---------------------")
	} else {
		buf.WriteString(pe.Message)
	}
	buf.WriteString("\n")
	pe.Call.Format(buf)
	buf.WriteString("\n")

	if pe.Parent != nil {
		buf.WriteString("~")

		switch po := pe.Parent.(type) {
		case *PointingError:
			po.Format(buf)
		default:
			buf.WriteString(po.Error())
		}
	}
}

//**************************************************************
// StackTrace
//**************************************************************

// StackTrace embodies data related to retrieved
// stack collected from runtime.
type StackTrace struct {
	Stack []byte    `json:"stack"`
	Time  time.Time `json:"end_time"`
}

// Stack returns a StackTrace containing time and stack slice
// for function calls within called area based on size desired.
func Stack(size int) *StackTrace {
	if size == 0 {
		size = stackSize
	}

	trace := make([]byte, size)
	trace = trace[:runtime.Stack(trace, false)]
	return &StackTrace{
		Stack: trace,
		Time:  time.Now(),
	}
}

// String returns the giving trace timestamp for the execution time.
func (t StackTrace) String() string {
	return fmt.Sprintf("[Time=%q] %+s", t.Time, t.Stack)
}

//**************************************************************
// GetMethod
//**************************************************************

// Location defines the location which an history occured in.
type Location struct {
	Function string `json:"function"`
	Line     int    `json:"line"`
	File     string `json:"file"`
}

// FormatBy formats giving location and writes to provided buffer.
func (c *Location) FormatBy(by *bytes.Buffer) {
	by.WriteString(fmt.Sprintf("--- [%q]:\n  %s:%d\n", c.Function, c.File, c.Line))
}

// CallGraph embodies a graph representing the areas where a method
// call occured.
type CallGraph struct {
	In    Location
	By    Location
	Stack []Location
}

// Format writes giving Callgraph into byte Buffer.
func (c *CallGraph) Format(by *bytes.Buffer) {
	c.By.FormatBy(by)
	c.In.FormatBy(by)
	for _, ll := range c.Stack {
		ll.FormatBy(by)
	}
}

// GetMethod returns the caller of the function that called it :)
func GetMethod(depth int) (string, string, int) {
	// we get the callers as uintptrs - but we just need 1
	fpcs := make([]uintptr, 1)

	// skip 3 levels to get to the caller of whoever called Caller()
	n := runtime.Callers(depth, fpcs)
	if n == 0 {
		return unknownName, unknownFile, 0
	}

	funcPtr := fpcs[0]
	funcPtrArea := funcPtr - 1

	// get the info of the actual function that's in the pointer
	fun := runtime.FuncForPC(funcPtrArea)
	if fun == nil {
		return unknownName, unknownFile, 0
	}

	fileName, line := fun.FileLine(funcPtrArea)

	// return its name
	return fun.Name(), fileName, line
}

// GetMethodGraph returns the caller of the function that called it :)
func GetMethodGraph(depth int, stack int) CallGraph {
	var graph CallGraph
	graph.In.File = unknownFile
	graph.By.File = unknownFile
	graph.In.Function = unknownName
	graph.By.Function = unknownName
	graph.Stack = make([]Location, 0, stack)

	// we get the callers as uintptrs - but we just need 1
	lower := make([]uintptr, stack+2)

	// skip 3 levels to get to the caller of whoever called Caller()
	if n := runtime.Callers(depth, lower); n == 0 {
		return graph
	}

	lowerPtr := lower[0] - 1
	higherPtr := lower[1] - 1

	// get the info of the actual function that's in the pointer
	if lowerFun := runtime.FuncForPC(lowerPtr); lowerFun != nil {
		graph.By.File, graph.By.Line = lowerFun.FileLine(lowerPtr)
		graph.By.Function = lowerFun.Name()

	}

	if higherFun := runtime.FuncForPC(higherPtr); higherFun != nil {
		graph.In.File, graph.In.Line = higherFun.FileLine(higherPtr)
		graph.In.Function = higherFun.Name()
	}

	for i := 2; i < len(lower); i++ {
		ptr := lower[i] - 1
		if mfun := runtime.FuncForPC(ptr); mfun != nil {
			var fm Location
			fm.File, fm.Line = mfun.FileLine(ptr)
			fm.Function = mfun.Name()
			graph.Stack = append(graph.Stack, fm)
		}
	}

	return graph
}

//***************************************
// internal types and functions
//***************************************

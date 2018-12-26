package actorkit

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
)

//***************************************************************************
// LogEvent
//***************************************************************************

// Level defines different level warnings for giving
// log events.
type Level uint8

// constants of log levels this package respect.
// They are capitalize to ensure no naming conflict.
const (
	INFO Level = 1 << iota
	DEBUG
	WARN
	ERROR
	PANIC
)

// String implements the Stringer interface.
func (l Level) String() string {
	switch l {
	case INFO:
		return "INFO"
	case ERROR:
		return "ERROR"
	case DEBUG:
		return "DEBUG"
	case WARN:
		return "WARN"
	case PANIC:
		return "PANIC"
	}
	return "UNKNOWN"
}

// LogMessage defines an interface which exposes a method for retrieving
// log details for giving log item.
type LogMessage interface {
	Message() string
}

// Logs defines a acceptable logging interface which all elements and sub packages
// will respect and use to deliver logs for different parts and ops, this frees
// this package from specifying or locking a giving implementation and contaminating
// import paths. Implement this and pass in to elements that provide for it.
type Logs interface {
	Emit(Level, LogMessage)
}

// ContextLogs defines an interface that returns a Logs which exposes a method to return
// a logger which contextualizes the provided actor as a base for it's logger.
type ContextLogs interface {
	Get(Actor) Logs
}

//*****************************************************************
// DrainLog
//*****************************************************************

// DrainLog implements the actorkit.Logs interface.
type DrainLog struct{}

// Emit does nothing with provided arguments, it implements
// actorkit.Logs Emit method.
func (DrainLog) Emit(_ Level, _ LogMessage) {}

//*****************************************************************
// ContextLogFn
//*****************************************************************

// ContextLogFn implements the ContextLogs interface. It uses
// a provided function which returns a appropriate logger for
// a giving actor.
type ContextLogFn struct {
	Fn func(Actor) Logs
}

// NewContextLogFn returns a new instance of ContextLogFn
func NewContextLogFn(fn func(Actor) Logs) *ContextLogFn {
	return &ContextLogFn{Fn: fn}
}

// Get calls the underline function and returns the produced logger
// for the passed in actor.
func (c ContextLogFn) Get(a Actor) Logs {
	return c.Fn(a)
}

//*****************************************************************
// LogEvent
//*****************************************************************

var (
	comma        = []byte(",")
	colon        = []byte(":")
	space        = []byte(" ")
	openBlock    = []byte("{")
	closingBlock = []byte("}")
	doubleQuote  = []byte("\"")
	logEventPool = sync.Pool{
		New: func() interface{} {
			return &logEventImpl{content: make([]byte, 0, 512), r: 1}
		},
	}
)

// LogEvent exposes set methods for generating a safe low-allocation json log
// based on a set of messages and key-value pairs.
type LogEvent interface {
	Write() LogMessage

	Int(string, int) LogEvent
	Bool(string, bool) LogEvent
	Int64(string, int64) LogEvent
	Bytes(string, []byte) LogEvent
	QBytes(string, []byte) LogEvent
	String(string, string) LogEvent
	Float64(string, float64) LogEvent
	Object(string, func(LogEvent)) LogEvent
	ObjectJSON(string, interface{}) LogEvent
}

// LogMsg requests allocation for a LogEvent from the internal pool returning a LogEvent for use
// which must be have it's Write() method called once done.
func LogMsg(message string) LogEvent {
	event := logEventPool.Get().(*logEventImpl)
	event.reset()
	event.addQuotedString("message", message)
	event.endEntry()
	return event
}

// LogMsgWithContext requests allocation for a LogEvent from the internal pool returning a LogEvent
// for use. It packs the field into a internal map with the key for that map set to the value of ctx.
// which must be have it's Write() method called once done.
//
// If a hook is provided then the hook is used to add field key-value pairs to the root of the
// returned json.
func LogMsgWithContext(message string, ctx string, hook func(LogEvent)) LogEvent {
	event := logEventPool.Get().(*logEventImpl)
	event.reset()
	event.onRelease = func(s string) string {
		newEvent := logEventPool.Get().(*logEventImpl)
		newEvent.reset()

		newEvent.addQuotedString("message", message)
		newEvent.endEntry()

		if hook != nil {
			hook(newEvent)
		}

		newEvent.addString(ctx, s)
		newEvent.endEntry()

		return newEvent.Write().Message()
	}
	return event
}

// logEventImpl implements a efficient zero or near zero-allocation as much as possible,
// using a underline non-strict json format to transform log key-value pairs into
// a  LogMessage.
//
// Each logEventImpl iss retrieved from a pool and will panic if after release/write it is used.
type logEventImpl struct {
	r         uint32
	content   []byte
	onRelease func(string) string
}

// String adds a field name with string value.
func (l *logEventImpl) String(name string, value string) LogEvent {
	l.addQuotedString(name, value)
	l.endEntry()
	return l
}

// Bytes adds a field name with bytes value. The byte is expected to be
// valid JSON, no checks are made to ensure this, you can mess up your JSON
// if you do not use this correctly.
func (l *logEventImpl) Bytes(name string, value []byte) LogEvent {
	l.addBytes(name, value)
	l.endEntry()
	return l
}

// QBytes adds a field name with bytes value. The byte is expected to be
// will be wrapped with quotation.
func (l *logEventImpl) QBytes(name string, value []byte) LogEvent {
	l.addQuotedBytes(name, value)
	l.endEntry()
	return l
}

// Object adds a field name with object value.
func (l *logEventImpl) Object(name string, handler func(event LogEvent)) LogEvent {
	newEvent := &logEventImpl{content: make([]byte, 0, 512), r: 1}
	defer logEventPool.Put(newEvent)

	newEvent.begin()
	handler(newEvent)
	total := len(comma) + len(space)
	newEvent.reduce(total)
	newEvent.end()

	l.addBytes(name, newEvent.Buf())
	l.endEntry()

	newEvent.resetContent()
	newEvent.release()
	return l
}

// ObjectJSON adds a field name with object value.
func (l *logEventImpl) ObjectJSON(name string, value interface{}) LogEvent {
	data, err := json.Marshal(value)
	if err != nil {
		fmt.Printf("JSON Marshalling %#v with failure: %+s\n", value, err)
		return l
	}

	l.addBytes(name, data)
	l.endEntry()
	return l
}

// Bool adds a field name with bool value.
func (l *logEventImpl) Bool(name string, value bool) LogEvent {
	l.addString(name, strconv.FormatBool(value))
	l.endEntry()
	return l
}

// Int adds a field name with int value.
func (l *logEventImpl) Int(name string, value int) LogEvent {
	l.addString(name, strconv.Itoa(value))
	l.endEntry()
	return l
}

// In64 adds a field name with int64 value.
func (l *logEventImpl) Int64(name string, value int64) LogEvent {
	l.addString(name, strconv.FormatInt(value, 64))
	l.endEntry()
	return l
}

// Float64 adds a field name with float64 value.
func (l *logEventImpl) Float64(name string, value float64) LogEvent {
	l.addString(name, strconv.FormatFloat(value, 'E', -1, 64))
	l.endEntry()
	return l
}

// Write delivers giving log event as a generated message.
func (l *logEventImpl) Write() LogMessage {
	if l.released() {
		panic("Re-using released logEventImpl")
	}

	// remove last comma and space
	total := len(comma) + len(space)
	l.reduce(total)
	l.end()

	content := string(l.content)
	if l.onRelease != nil {
		content = l.onRelease(content)
		l.onRelease = nil
	}

	l.resetContent()
	l.release()
	return Message(content)
}

// Buf returns the current content of the logEventImpl.
func (l *logEventImpl) Buf() []byte {
	return l.content
}

func (l *logEventImpl) reset() {
	atomic.StoreUint32(&l.r, 1)
	l.begin()
}

func (l *logEventImpl) reduce(d int) {
	available := len(l.content)
	l.content = l.content[:available-d]
}

func (l *logEventImpl) resetContent() {
	l.content = l.content[:0]
}

func (l *logEventImpl) released() bool {
	return atomic.LoadUint32(&l.r) == 0
}

func (l *logEventImpl) release() {
	logEventPool.Put(l)
	atomic.StoreUint32(&l.r, 0)
}

func (l *logEventImpl) begin() {
	l.content = append(l.content, openBlock...)
}

func (l *logEventImpl) addQuotedString(k string, v string) {
	if l.released() {
		panic("Re-using released logEventImpl")
	}

	l.content = append(l.content, doubleQuote...)
	l.content = append(l.content, k...)
	l.content = append(l.content, doubleQuote...)
	l.content = append(l.content, colon...)
	l.content = append(l.content, space...)
	l.content = append(l.content, doubleQuote...)
	l.content = append(l.content, v...)
	l.content = append(l.content, doubleQuote...)
}

func (l *logEventImpl) addString(k string, v string) {
	if l.released() {
		panic("Re-using released logEventImpl")
	}

	l.content = append(l.content, doubleQuote...)
	l.content = append(l.content, k...)
	l.content = append(l.content, doubleQuote...)
	l.content = append(l.content, colon...)
	l.content = append(l.content, space...)
	l.content = append(l.content, v...)
}

func (l *logEventImpl) addQuotedBytes(k string, v []byte) {
	if l.released() {
		panic("Re-using released logEventImpl")
	}

	l.content = append(l.content, doubleQuote...)
	l.content = append(l.content, k...)
	l.content = append(l.content, doubleQuote...)
	l.content = append(l.content, colon...)
	l.content = append(l.content, space...)
	l.content = append(l.content, doubleQuote...)
	l.content = append(l.content, v...)
	l.content = append(l.content, doubleQuote...)
}

func (l *logEventImpl) addBytes(k string, v []byte) {
	if l.released() {
		panic("Re-using released logEventImpl")
	}

	l.content = append(l.content, doubleQuote...)
	l.content = append(l.content, k...)
	l.content = append(l.content, doubleQuote...)
	l.content = append(l.content, colon...)
	l.content = append(l.content, space...)
	l.content = append(l.content, v...)
}

func (l *logEventImpl) endEntry() {
	l.content = append(l.content, comma...)
	l.content = append(l.content, space...)
}

func (l *logEventImpl) end() {
	l.content = append(l.content, closingBlock...)
}

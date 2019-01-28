package actorkit

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"
)

//*****************************************************************
// *LogEvent
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
			return &LogEvent{content: make([]byte, 0, 218), r: 1}
		},
	}
)

// LogMsg requests allocation for a *LogEvent from the internal pool returning a *LogEvent for use
// which must be have it's Write() method called once done.
func LogMsg(message string, inherits ...func(event *LogEvent)) *LogEvent {
	event := logEventPool.Get().(*LogEvent)
	event.reset()
	event.addQuotedString("message", message)
	event.endEntry()

	for _, op := range inherits {
		op(event)
	}

	return event
}

// LogMsgWithContext requests allocation for a *LogEvent from the internal pool returning a *LogEvent
// for use. It packs the field into a internal map with the key for that map set to the value of ctx.
// which must be have it's Write() method called once done.
//
// If a hook is provided then the hook is used to add field key-value pairs to the root of the
// returned json.
func LogMsgWithContext(message string, ctx string, hook func(*LogEvent), inherits ...func(event *LogEvent)) *LogEvent {
	event := logEventPool.Get().(*LogEvent)
	event.reset()
	event.onRelease = func(s []byte) []byte {
		newEvent := logEventPool.Get().(*LogEvent)
		newEvent.reset()

		newEvent.addQuotedString("message", message)
		newEvent.endEntry()

		if hook != nil {
			hook(newEvent)
		}

		newEvent.addBytes(ctx, s)
		newEvent.end()

		content := newEvent.content
		newEvent.content = make([]byte, 0, 512)
		newEvent.release()
		return content
	}

	for _, op := range inherits {
		op(event)
	}
	return event
}

// *LogEvent implements a efficient zero or near zero-allocation as much as possible,
// using a underline non-strict json format to transform log key-value pairs into
// a  LogMessage.
//
// Each *LogEvent iss retrieved from a pool and will panic if after release/write it is used.
type LogEvent struct {
	r         uint32
	content   []byte
	onRelease func([]byte) []byte
}

// String adds a field name with string value.
func (l *LogEvent) String(name string, value string) *LogEvent {
	l.addQuotedBytes(name, string2Bytes(value))
	l.endEntry()
	return l
}

// Bytes adds a field name with bytes value. The byte is expected to be
// valid JSON, no checks are made to ensure this, you can mess up your JSON
// if you do not use this correctly.
func (l *LogEvent) Bytes(name string, value []byte) *LogEvent {
	l.addBytes(name, value)
	l.endEntry()
	return l
}

// QBytes adds a field name with bytes value. The byte is expected to be
// will be wrapped with quotation.
func (l *LogEvent) QBytes(name string, value []byte) *LogEvent {
	l.addQuotedBytes(name, value)
	l.endEntry()
	return l
}

// With applies giving function to the log event object.
func (l *LogEvent) With(handler func(event *LogEvent)) *LogEvent {
	handler(l)
	return l
}

// Object adds a field name with object value.
func (l *LogEvent) Object(name string, handler func(event *LogEvent)) *LogEvent {
	newEvent := logEventPool.Get().(*LogEvent)
	newEvent.reset()

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
func (l *LogEvent) ObjectJSON(name string, value interface{}) *LogEvent {
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
func (l *LogEvent) Bool(name string, value bool) *LogEvent {
	l.addString(name, strconv.FormatBool(value))
	l.endEntry()
	return l
}

// Int adds a field name with int value.
func (l *LogEvent) Int(name string, value int) *LogEvent {
	l.addString(name, strconv.Itoa(value))
	l.endEntry()
	return l
}

// In64 adds a field name with int64 value.
func (l *LogEvent) Int64(name string, value int64) *LogEvent {
	l.addString(name, strconv.FormatInt(value, 64))
	l.endEntry()
	return l
}

// Float64 adds a field name with float64 value.
func (l *LogEvent) Float64(name string, value float64) *LogEvent {
	l.addString(name, strconv.FormatFloat(value, 'E', -1, 64))
	l.endEntry()
	return l
}

// Message returns the generated JSON of giving *LogEvent.
func (l *LogEvent) Message() string {
	if l.released() {
		panic("Re-using released *LogEvent")
	}

	// remove last comma and space
	total := len(comma) + len(space)
	l.reduce(total)
	l.end()

	if l.onRelease != nil {
		l.content = l.onRelease(l.content)
		l.onRelease = nil
	}

	cn := make([]byte, len(l.content))
	copy(cn, l.content)

	l.resetContent()
	l.release()
	return bytes2String(cn)
}

// Write delivers giving log event as a generated message.
func (l *LogEvent) Write(ll Level, lg Logs) {
	lg.Emit(ll, Message(l.Message()))
}

// Buf returns the current content of the *LogEvent.
func (l *LogEvent) Buf() []byte {
	return l.content
}

func (l *LogEvent) reset() {
	atomic.StoreUint32(&l.r, 1)
	l.begin()
}

func (l *LogEvent) reduce(d int) {
	available := len(l.content)
	rem := available - d
	if rem < 0 {
		rem = 0
	}
	l.content = l.content[:rem]
}

func (l *LogEvent) resetContent() {
	l.content = l.content[:0]
}

func (l *LogEvent) released() bool {
	return atomic.LoadUint32(&l.r) == 0
}

func (l *LogEvent) release() {
	atomic.StoreUint32(&l.r, 0)
	logEventPool.Put(l)
}

func (l *LogEvent) begin() {
	l.content = append(l.content, openBlock...)
}

func (l *LogEvent) addQuotedString(k string, v string) {
	if l.released() {
		panic("Re-using released *LogEvent")
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

func (l *LogEvent) addString(k string, v string) {
	if l.released() {
		panic("Re-using released *LogEvent")
	}

	l.content = append(l.content, doubleQuote...)
	l.content = append(l.content, k...)
	l.content = append(l.content, doubleQuote...)
	l.content = append(l.content, colon...)
	l.content = append(l.content, space...)
	l.content = append(l.content, v...)
}

func (l *LogEvent) addQuotedBytes(k string, v []byte) {
	if l.released() {
		panic("Re-using released *LogEvent")
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

func (l *LogEvent) addBytes(k string, v []byte) {
	if l.released() {
		panic("Re-using released *LogEvent")
	}

	l.content = append(l.content, doubleQuote...)
	l.content = append(l.content, k...)
	l.content = append(l.content, doubleQuote...)
	l.content = append(l.content, colon...)
	l.content = append(l.content, space...)
	l.content = append(l.content, v...)
}

func (l *LogEvent) endEntry() {
	l.content = append(l.content, comma...)
	l.content = append(l.content, space...)
}

func (l *LogEvent) end() {
	l.content = append(l.content, closingBlock...)
}

func bytes2String(bc []byte) string {
	return *(*string)(unsafe.Pointer(&bc))
}

func string2Bytes(bc string) []byte {
	return *(*[]byte)(unsafe.Pointer(&bc))
}

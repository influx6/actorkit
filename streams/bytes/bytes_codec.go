package bytes

import (
	"bufio"
	"bytes"
	"io"
	"sync/atomic"

	"github.com/gokit/actorkit/streams/pbytes"
	"github.com/gokit/errors"
)

var (
	spacePool = pbytes.NewBitsPool(32, 2)
)

//****************************************************************************
// DelimitedStreamWriter
//****************************************************************************

// DelimitedStreamWriter implements a giving byte encoder which will append
// to a giving set of byte after closure of writer with a ending delimiter
// as separator. It escapes all appearance of that giving byte using the escape
// byte value provided.
type DelimitedStreamWriter struct {
	Dest      io.Writer
	Escape    []byte
	Delimiter []byte

	count  int64
	buffer *bytes.Buffer
	cache  *bufio.Writer
}

// WriteByte writes individual byte element into underline stream,
// ensuring to adequately escape all appearing delimiter within
// writing stream.
func (dw *DelimitedStreamWriter) WriteByte(b byte) error {
	if dw.buffer == nil && dw.cache == nil {
		escapeLen := len(dw.Escape)
		delimLen := len(dw.Delimiter)

		dw.buffer = bytes.NewBuffer(make([]byte, 0, delimLen))
		dw.cache = bufio.NewWriterSize(dw.Dest, (escapeLen*2)+delimLen)
	}

	// Does the giving byte match our delimiters first character?
	// if so, cache in buffer till we have full buffer set to compare.
	if dw.buffer.Len() == 0 && b == dw.Delimiter[0] {
		return dw.buffer.WriteByte(b)
	}

	// if we are empty and do not match set then write
	if dw.buffer.Len() == 0 && b != dw.Delimiter[0] {
		return dw.cache.WriteByte(b)
	}

	space := dw.buffer.Cap() - dw.buffer.Len()

	// if we are now collecting possible match found delimiter
	// within incoming b stream, then we keep collecting till
	// we have enough to check against.
	if dw.buffer.Len() != 0 && dw.buffer.Len() < dw.buffer.Cap() {
		if err := dw.buffer.WriteByte(b); err != nil {
			return err
		}

		// if we were left with a single space then this was filled, hence then
		// it's time to check and flush.
		if space == 1 {
			return dw.flush()
		}
	}

	return nil
}

// End adds delimiter to underline writer to indicate end of byte stream section.
// This allows us indicate to giving stream as ending as any other occurrence of giving
// stream is closed.
func (dw *DelimitedStreamWriter) End() (int, error) {
	written := int(atomic.LoadInt64(&dw.count))
	atomic.StoreInt64(&dw.count, 0)

	if err := dw.flush(); err != nil {
		return written, err
	}

	n, err := dw.Dest.Write(dw.Delimiter)
	if err != nil {
		return written, err
	}

	written += n
	return written, nil
}

// Write implements the io.Writer interface and handles the writing of
// a byte slice in accordance with escaping and delimiting rule.
func (dw *DelimitedStreamWriter) Write(bs []byte) (int, error) {
	var count int
	for _, b := range bs {
		if err := dw.WriteByte(b); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

// flush compares existing buffered data if it matches delimiter, escapes
// it and flushes into buffered writer else, flushes buffered data into
// buffered writer and continues streaming.
func (dw *DelimitedStreamWriter) flush() error {
	if bytes.Equal(dw.buffer.Bytes(), dw.Delimiter) {
		escapeN, err := dw.cache.Write(dw.Escape)
		if err != nil {
			return err
		}

		delimN, err := dw.cache.Write(dw.Delimiter)
		if err != nil {
			return err
		}

		atomic.AddInt64(&dw.count, int64(escapeN+delimN))
		return nil
	}

	next, err := dw.cache.Write(dw.buffer.Bytes())
	if err != nil {
		return err
	}

	atomic.AddInt64(&dw.count, int64(next))
	dw.buffer.Reset()
	return nil
}

//****************************************************************************
// DelimitedStreamReader
//****************************************************************************

const defaultBuffer = 4096

var (
	// ErrEOS is sent when a giving byte stream section is reached, that is
	// we've found the ending delimiter representing the end of a message stream
	// among incoming multi-duplexed stream.
	ErrEOS = errors.New("end of stream set")
)

// DelimitedStreamReader continuously reads incoming byte sequence decoding
// them by unwrapping cases where delimiter was escaped as it appeared as part
// of normal byte stream. It ends it's encoding when giving delimiter is read.
type DelimitedStreamReader struct {
	Src        io.Reader
	Escape     []byte
	Delimiter  []byte
	ReadBuffer int

	buffer  *bufio.Reader
	escaped *bytes.Buffer
	cached  *bytes.Buffer
}

// Read implements the io.Read interface providing writing of incoming
// byte sequence from underline reader, transforming and un-escaping
// escaped byte sequencing and writing result into passed byte slice.
func (dr *DelimitedStreamReader) Read(b []byte) (int, error) {
	var c int
	return c, nil
}

// ReadByte reads incoming byte sequence from internal reader.
// It will skip appearance of escape character for escaped delimiter
// bytes, returning only delimiter when escaped and skipping delimiter
// which is meant to represent end of byte sequence.
func (dr *DelimitedStreamReader) ReadByte() (byte, error) {
	if dr.cached == nil && dr.buffer == nil {
		escapeLen := len(dr.Escape)
		delimLen := len(dr.Delimiter)

		if dr.ReadBuffer == 0 {
			dr.ReadBuffer = defaultBuffer
		}

		dr.buffer = bufio.NewReaderSize(dr.Src, dr.ReadBuffer)
		dr.escaped = bytes.NewBuffer(make([]byte, 0, escapeLen))
		dr.cached = bytes.NewBuffer(make([]byte, 0, delimLen))
	}

	var r byte
	return r, nil
}

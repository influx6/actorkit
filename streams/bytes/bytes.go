package bytes

import (
	"bufio"
	"bytes"
	"io"
	"sync/atomic"

	"github.com/gokit/errors"
)

var (
	// ErrInvalidEscapeAndDelimiter defines error returned when delimiter and escape values
	// are the same.
	ErrInvalidEscapeAndDelimiter = errors.New("delimiter and escape values can not be the same")
)

//****************************************************************************
// DelimitedStreamWriter
//****************************************************************************

// DelimitedStreamWriter implements a giving byte encoder which will append
// to a giving set of byte after closure of writer with a ending delimiter
// as separator. It escapes all appearance of that giving byte using the escape
// byte value provided.
//
// It's not safe for concurrent use.
type DelimitedStreamWriter struct {
	Dest        io.Writer
	Escape      []byte
	Delimiter   []byte
	WriteBuffer int

	count  int64
	index  int
	buffer *bytes.Buffer
	escape *bytes.Buffer
	cache  *bufio.Writer
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

// End adds delimiter to underline writer to indicate end of byte stream section.
// This allows us indicate to giving stream as ending as any other occurrence of giving
// stream is closed.
func (dw *DelimitedStreamWriter) End() (int, error) {
	written := int(atomic.LoadInt64(&dw.count))
	atomic.StoreInt64(&dw.count, 0)

	// if we still have data within buffer, write to cache.
	//if dw.buffer.Len() != 0 {
	//	next, err := dw.buffer.WriteTo(dw.cache)
	//	if err != nil {
	//		written += int(next)
	//		return written, errors.WrapOnly(err)
	//	}
	//
	//	written += int(next)
	//}

	if err := dw.flush(); err != nil {
		return written, err
	}

	// flush buffer if we have something left as well into writer.
	if left := dw.cache.Buffered(); left > 0 {
		if err := dw.cache.Flush(); err != nil {
			return written, errors.WrapOnly(err)
		}
	}

	n, err := dw.Dest.Write(dw.Delimiter)
	if err != nil {
		return written, errors.WrapOnly(err)
	}

	written += n

	dw.index = 0
	dw.buffer.Reset()
	dw.escape.Reset()
	atomic.StoreInt64(&dw.count, 0)
	return written, nil
}

// WriteByte writes individual byte element into underline stream,
// ensuring to adequately escape all appearing delimiter within
// writing stream.
func (dw *DelimitedStreamWriter) WriteByte(b byte) error {
	escapeLen := len(dw.Escape)
	delimLen := len(dw.Delimiter)
	if dw.buffer == nil && dw.cache == nil {
		if bytes.Equal(dw.Escape, dw.Delimiter) {
			return errors.WrapOnly(ErrInvalidEscapeAndDelimiter)
		}

		writeBuffer := dw.WriteBuffer
		if writeBuffer <= 0 {
			writeBuffer = defaultBuffer
		}

		possibleSize := (escapeLen * 2) + delimLen
		if writeBuffer < possibleSize {
			writeBuffer += possibleSize
		}

		dw.escape = bytes.NewBuffer(make([]byte, 0, escapeLen))
		dw.buffer = bytes.NewBuffer(make([]byte, 0, delimLen))
		dw.cache = bufio.NewWriterSize(dw.Dest, writeBuffer)
	}

	// if we have not started buffering normally and we found escape character
	// then use escape logic.
	if dw.buffer.Len() == 0 && dw.escape.Len() == 0 && b == dw.Escape[dw.index] {
		atomic.AddInt64(&dw.count, 1)
		dw.index++
		if err := dw.escape.WriteByte(b); err != nil {
			return errors.WrapOnly(err)
		}
		return nil
	}

	// if the next one does not matches our escape character again, flush and move down wards
	// to delimiter checks.
	if dw.buffer.Len() == 0 && dw.escape.Len() != 0 && b != dw.Escape[dw.index] {
		if _, err := dw.escape.WriteTo(dw.cache); err != nil {
			return errors.WrapOnly(err)
		}

		dw.index = 0
		dw.escape.Reset()
	}

	// if the next one matches our escape character again, continue written to escape.
	if dw.buffer.Len() == 0 && dw.escape.Len() != 0 && b == dw.Escape[dw.index] {
		atomic.AddInt64(&dw.count, 1)
		dw.index++

		if err := dw.escape.WriteByte(b); err != nil {
			return errors.WrapOnly(err)
		}

		if dw.escape.Len() == escapeLen {
			if _, err := dw.escape.WriteTo(dw.cache); err != nil {
				return errors.WrapOnly(err)
			}

			dw.index = 0
			dw.escape.Reset()
		}

		return nil
	}

	// if we are empty and do not match set then write
	if dw.buffer.Len() == 0 && b != dw.Delimiter[0] {
		atomic.AddInt64(&dw.count, 1)
		if err := dw.cache.WriteByte(b); err != nil {
			return errors.WrapOnly(err)
		}
		return nil
	}

	// Does the giving byte match our delimiters first character?
	// if so, cache in buffer till we have full buffer set to compare.
	if dw.buffer.Len() == 0 && b == dw.Delimiter[0] {
		atomic.AddInt64(&dw.count, 1)
		if err := dw.buffer.WriteByte(b); err != nil {
			return errors.WrapOnly(err)
		}
		return nil
	}

	space := dw.buffer.Cap() - dw.buffer.Len()

	// if we are now collecting possible match found delimiter
	// within incoming b stream, then we keep collecting till
	// we have enough to check against.
	if dw.buffer.Len() != 0 && dw.buffer.Len() < dw.buffer.Cap() {
		atomic.AddInt64(&dw.count, 1)

		if err := dw.buffer.WriteByte(b); err != nil {
			return errors.WrapOnly(err)
		}

		// if we were left with a single space then this was filled, hence then
		// it's time to check and flush.
		if space == 1 {
			return dw.flush()
		}
	}

	return nil
}

// flush compares existing buffered data if it matches delimiter, escapes
// it and flushes into buffered writer else, flushes buffered data into
// buffered writer and continues streaming.
func (dw *DelimitedStreamWriter) flush() error {
	if bytes.Equal(dw.buffer.Bytes(), dw.Delimiter) {
		escapeN, err := dw.cache.Write(dw.Escape)
		if err != nil {
			return errors.WrapOnly(err)
		}

		atomic.AddInt64(&dw.count, int64(escapeN))

		if _, err := dw.cache.Write(dw.Delimiter); err != nil {
			return errors.WrapOnly(err)
		}

		dw.buffer.Reset()
		return nil
	}

	next, err := dw.buffer.WriteTo(dw.cache)
	if err != nil {
		return errors.WrapOnly(err)
	}

	atomic.AddInt64(&dw.count, int64(next))
	return nil
}

//****************************************************************************
// DelimitedStreamReader
//****************************************************************************

const defaultBuffer = 1024

var (
	// ErrEOS is sent when a giving byte stream section is reached, that is
	// we've found the ending delimiter representing the end of a message stream
	// among incoming multi-duplexed stream.
	ErrEOS = errors.New("end of stream set")
)

// DelimitedStreamReader continuously reads incoming byte sequence decoding
// them by unwrapping cases where delimiter was escaped as it appeared as part
// of normal byte stream. It ends it's encoding when giving delimiter is read.
//
// It's not safe for concurrent use.
type DelimitedStreamReader struct {
	Src        io.Reader
	Escape     []byte
	Delimiter  []byte
	ReadBuffer int

	index     int
	escapes   int
	comparing bool
	keepRead  bool
	unit      bool
	unitLen   int

	buffer *bufio.Reader
	cached *bytes.Buffer
}

// Read implements the io.Read interface providing writing of incoming
// byte sequence from underline reader, transforming and un-escaping
// escaped byte sequencing and writing result into passed byte slice.
func (dr *DelimitedStreamReader) Read(b []byte) (int, error) {
	var count int
	for {
		if dr.keepRead {
			n, err := dr.cached.Read(b)
			if dr.cached.Len() == 0 {
				dr.keepRead = false
			}

			count += n

			if err != nil {
				return count, err
			}

			if dr.cached.Len() == 0 {
				// if this is a unit, meaning a set unit of a stream
				// in a multi-stream, return EOS (End of stream) error
				// to signal end of a giving stream.
				if dr.unit {
					dr.unit = false
					return count, ErrEOS
				}
			}

			// if we maxed out byte space, return.
			if n == len(b) {
				// if this is a unit, meaning a set unit of a stream
				// in a multi-stream, return EOS (End of stream) error
				// to signal end of a giving stream.
				if dr.unit {
					dr.unit = false
					return count, ErrEOS
				}

				return count, nil
			}

			// skip written bytes and retry.
			b = b[n:]
			continue
		}

		space := len(b)

		// continuously attempt to read till we have found a possible
		// byte sequence or an error.
		state, err := dr.readTill(space)
		if err != nil {

			// if an error occurred, read what we have in cache
			// and return error.
			if dr.cached.Len() > 0 {
				n, _ := dr.cached.Read(b)
				count += n
				return count, err
			}

			return count, err
		}

		// if we have found a possible stepping area
		// switch on exhausting reading and read till
		// cache is empty.
		if state {
			dr.keepRead = true
			continue
		}
	}
}

func (dr *DelimitedStreamReader) readTill(space int) (bool, error) {
	escapeLen := len(dr.Escape)
	delimLen := len(dr.Delimiter)
	delims := escapeLen + delimLen
	if dr.cached == nil && dr.buffer == nil {
		if bytes.Equal(dr.Escape, dr.Delimiter) {
			return false, errors.WrapOnly(ErrInvalidEscapeAndDelimiter)
		}

		readBuffer := dr.ReadBuffer
		if readBuffer <= 0 {
			readBuffer = defaultBuffer
		}
		if readBuffer < delims {
			readBuffer += delims
		}

		dr.buffer = bufio.NewReaderSize(dr.Src, readBuffer)

		cacheBuffer := space
		if cacheBuffer < defaultBuffer {
			cacheBuffer = defaultBuffer
		}
		dr.cached = bytes.NewBuffer(make([]byte, 0, cacheBuffer))
	}

	// if we are around available space for reading, then return true
	// and let element be read into provided buffer.
	if dr.cached.Len() >= space {
		return true, nil
	}

	// if we are comparing and we reached end of escape index,
	// we need to check if next set matches delimiter, then
	// add delimiter as being escaped.
	if dr.comparing && dr.index >= escapeLen {
		next, err := dr.buffer.Peek(delimLen)
		if err != nil {
			return false, errors.WrapOnly(err)
		}

		if bytes.Equal(dr.Delimiter, next) {
			dr.index = 0
			dr.comparing = false

			if _, err := dr.cached.Write(next); err != nil {
				return false, errors.WrapOnly(err)
			}

			if _, err := dr.buffer.Discard(delimLen); err != nil {
				return false, errors.WrapOnly(err)
			}

			return false, nil
		}

		// we properly just found a possible escape sequence showing up
		// and not a escaping of delimiter, so add then move on.
		dr.index = 0
		dr.comparing = false

		if _, err := dr.cached.Write(dr.Escape); err != nil {
			return false, errors.WrapOnly(err)
		}

		return false, nil
	}

	bm, err := dr.buffer.ReadByte()
	if err != nil {
		return false, errors.WrapOnly(err)
	}

	// if we are not comparing and we don't match giving escape sequence
	// at index, then just cache and return.
	if !dr.comparing && bm != dr.Escape[dr.index] {

		// if we match delimiter first character here, then
		// check if we have other match.
		if bm == dr.Delimiter[0] {
			next, err := dr.buffer.Peek(delimLen - 1)
			if err != nil {
				return false, errors.WrapOnly(err)
			}

			if bytes.Equal(dr.Delimiter[1:], next) {
				dr.buffer.Discard(len(next))
				dr.unit = true
				return true, nil
			}
		}

		if err := dr.cached.WriteByte(bm); err != nil {
			return false, errors.WrapOnly(err)
		}

		return false, nil
	}

	// if not comparing and we match, increment index and
	// setup comparing to true then return.
	if !dr.comparing && bm == dr.Escape[dr.index] {
		dr.index++
		dr.comparing = true
		return false, nil
	}

	// if we are and the byte matches the next byte in escape, increment and
	// return.
	if dr.comparing && bm == dr.Escape[dr.index] {
		dr.index++
		return false, nil
	}

	// if we are comparing and we are at a point where
	// the next byte does not match our next sequence
	if dr.comparing && bm != dr.Escape[dr.index] {
		part := dr.Escape[:dr.index-1]

		dr.index = 0
		dr.comparing = false

		// write part of escape sequence that had being checked.
		if _, err := dr.cached.Write(part); err != nil {
			return false, errors.WrapOnly(err)
		}

		if err := dr.buffer.UnreadByte(); err != nil {
			return false, errors.WrapOnly(err)
		}
	}

	// if we are comparing and we have reached end of escape sequence, update
	// escape count, check if next sequence is delimiter, if so, skip escape
	// and write delimiter into cache as we are dealing with an escaped delimiter
	// sequence that appeared during encoding, return true to allow reading but
	// do not set unit as in unit stream to true.
	if dr.comparing && dr.index == len(dr.Escape) {
		dr.escapes++
		peekNext, err := dr.buffer.Peek(delimLen)
		if err != nil {
			return false, errors.WrapOnly(err)
		}

		// if it's delimiter, reduce escape, write out
		// pending escape and write delimiter.
		if bytes.Equal(peekNext, dr.Delimiter) {

			// skip delimiter found
			if _, err := dr.buffer.Discard(delimLen); err != nil {
				return false, errors.WrapOnly(err)
			}

			// reduce escape sequence to ensure we
			// can account for multiple occurrence
			if dr.escapes > 0 {
				dr.escapes--
			}

			// write out found escape.
			for i := dr.escapes; i > 0; i-- {
				dr.cached.Write(dr.Escape)
			}

			// write out found delimiter.
			dr.cached.Write(dr.Delimiter)
			dr.index = 0
			dr.escapes = 0
			dr.comparing = false

			return true, nil
		}
	}

	return false, nil
}

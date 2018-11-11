package bytes_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/gokit/errors"

	"github.com/stretchr/testify/assert"

	mb "github.com/gokit/actorkit/streams/bytes"
)

func TestDelimitedStreamWriterWithDelimiterAndEscape(t *testing.T) {
	var dest bytes.Buffer
	writer := &mb.DelimitedStreamWriter{
		Dest:      &dest,
		Escape:    []byte(":/"),
		Delimiter: []byte("//"),
	}

	data := []byte("Wondering out the :///clouds of endless //streams beyond the shore")
	written, err := writer.Write(data)
	assert.NoError(t, err)
	assert.Len(t, data, written)

	processed, err := writer.End()
	assert.NoError(t, err)
	assert.Equal(t, processed, dest.Len())

	expected := []byte("Wondering out the :/:///clouds of endless :///streams beyond the shore//")
	assert.Equal(t, expected, dest.Bytes())
}

func TestDelimitedStreamWriterWithAllDelimiter(t *testing.T) {
	var dest bytes.Buffer
	writer := &mb.DelimitedStreamWriter{
		Dest:      &dest,
		Escape:    []byte(":/"),
		Delimiter: []byte("//"),
	}

	data := []byte("Wondering out the ://////////////////////////////////////")
	written, err := writer.Write(data)
	assert.NoError(t, err)
	assert.Len(t, data, written)

	processed, err := writer.End()
	assert.NoError(t, err)
	assert.Equal(t, processed, dest.Len())

	expected := []byte("Wondering out the :/:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///://////")
	assert.Equal(t, expected, dest.Bytes(), "Expected %q to produce matched", data)
}

func TestDelimitedStreamWriterWithMoreEscapeWithDelimiter(t *testing.T) {
	var dest bytes.Buffer
	writer := &mb.DelimitedStreamWriter{
		Dest:      &dest,
		Escape:    []byte(":/"),
		Delimiter: []byte("//"),
	}

	data := []byte("Wondering out the :///clouds of endless //streams beyond the shore")
	written, err := writer.Write(data)
	assert.NoError(t, err)
	assert.Len(t, data, written)

	processed, err := writer.End()
	assert.NoError(t, err)
	assert.Equal(t, processed, dest.Len())

	expected := []byte("Wondering out the :/:///clouds of endless :///streams beyond the shore//")
	assert.Equal(t, expected, dest.Bytes())
}

func TestDelimitedStreamWriterWithDelimiter(t *testing.T) {
	var dest bytes.Buffer
	writer := &mb.DelimitedStreamWriter{
		Dest:      &dest,
		Escape:    []byte(":/"),
		Delimiter: []byte("//"),
	}

	data := []byte("Wondering out the //clouds of endless //streams beyond the shore")
	written, err := writer.Write(data)
	assert.NoError(t, err)
	assert.Len(t, data, written)

	processed, err := writer.End()
	assert.NoError(t, err)
	assert.Equal(t, processed, dest.Len())

	expected := []byte("Wondering out the :///clouds of endless :///streams beyond the shore//")
	assert.Equal(t, expected, dest.Bytes())
}

func TestDelimitedStreamWriter(t *testing.T) {
	var dest bytes.Buffer
	writer := &mb.DelimitedStreamWriter{
		Dest:      &dest,
		Escape:    []byte(":/"),
		Delimiter: []byte("//"),
	}

	data := []byte("Wondering out the clouds of endless streams beyond the shore")
	written, err := writer.Write(data)
	assert.NoError(t, err)
	assert.Len(t, data, written)

	processed, err := writer.End()
	assert.NoError(t, err)
	assert.Equal(t, processed, dest.Len())

	expected := []byte("Wondering out the clouds of endless streams beyond the shore//")
	assert.Equal(t, expected, dest.Bytes())
}

func TestDelimitedStreamWriterWithSet(t *testing.T) {
	specs := []struct {
		In  string
		Out string
		Err error
	}{
		{
			In:  "Wondering out the clouds of endless streams beyond the shore//",
			Out: "Wondering out the clouds of endless streams beyond the shore",
		},
		{
			In:  "Wondering out the :///clouds of endless :///streams beyond the shore//",
			Out: "Wondering out the //clouds of endless //streams beyond the shore",
		},
		{
			In:  "Wondering out the :/:///clouds of endless :///streams beyond the shore//",
			Out: "Wondering out the :///clouds of endless //streams beyond the shore",
		},
		{
			In:  "Wondering out the :/:///clouds of endless :///streams beyond the shore//",
			Out: "Wondering out the :///clouds of endless //streams beyond the shore",
		},
		{
			In:  "Wondering out the :/:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///://////",
			Out: "Wondering out the ://////////////////////////////////////",
		},
	}

	for ind, spec := range specs {
		dest := bytes.NewBuffer(make([]byte, 0, 128))
		writer := &mb.DelimitedStreamWriter{
			Dest:      dest,
			Escape:    []byte(":/"),
			Delimiter: []byte("//"),
		}

		writer.Write([]byte(spec.Out))

		_, err := writer.End()
		assert.NoError(t, err)
		assert.Equal(t, spec.In, string(dest.Bytes()), "Index %d\n Data: %q\nEncoded: %q", ind, spec.Out, dest.Bytes())
	}
}

func TestDelimitedStreamReaderWithSet(t *testing.T) {
	specs := []struct {
		In   string
		Out  string
		Err  error
		Fail bool
	}{
		{
			In:  "Wondering out the clouds of endless streams beyond the shore//",
			Out: "Wondering out the clouds of endless streams beyond the shore",
			Err: mb.ErrEOS,
		},
		{
			In:  "Wondering out the :///clouds of endless :///streams beyond the shore//",
			Out: "Wondering out the //clouds of endless //streams beyond the shore",
			Err: mb.ErrEOS,
		},
		{
			In:  "Wondering out the :/:///clouds of endless :///streams beyond the shore//",
			Out: "Wondering out the :///clouds of endless //streams beyond the shore",
			Err: mb.ErrEOS,
		},
		{
			In:  "Wondering out the :/:///clouds of endless :///streams beyond the shore//",
			Out: "Wondering out the :///clouds of endless //streams beyond the shore",
			Err: mb.ErrEOS,
		},
		{
			In:   "Wondering out the :/:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///:///",
			Out:  "Wondering out the ://///////////////////////////////////",
			Err:  io.EOF,
			Fail: true,
		},
	}

	for ind, spec := range specs {
		reader := &mb.DelimitedStreamReader{
			Src:       bytes.NewReader([]byte(spec.In)),
			Escape:    []byte(":/"),
			Delimiter: []byte("//"),
		}

		res := make([]byte, len(spec.In))
		read, err := reader.Read(res)
		assert.Error(t, err)
		assert.Equal(t, spec.Err, errors.UnwrapDeep(err))

		if !spec.Fail {
			assert.Equal(t, spec.Out, string(res[:read]), "Failed at index %d:\n In: %q\n Out: %q\n", ind, spec.In, res[:read])
		}
	}
}

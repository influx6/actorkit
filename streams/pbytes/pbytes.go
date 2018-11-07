package pbytes

import (
	"bytes"
	"sync"
)

type bufferHandler interface {
	Put(*Buffer)
}

//************************************************************************
// Buffer
//************************************************************************

// Buffer holds a allocated memory which will be used by
// receiver and discard once done with byte slice to
// allow re-use.
type Buffer struct {
	bit int
	Data []byte
	pool bufferHandler
}

func (b *Buffer) Discard(){
	b.pool.Put(b)
}

//************************************************************************
// BitsBoot
//************************************************************************

type bitsBoot struct {
	max  int
	pl sync.Mutex
	free []*Buffer
}

func (b *bitsBoot) Put(br *Buffer)  {
	b.pl.Lock()
	defer b.pl.Unlock()

	br.pool = nil
	b.free = append(b.free, br)
}

func (b *bitsBoot) Get(n int) *Buffer {
	b.pl.Lock()
	defer b.pl.Unlock()

	free := len(b.free)
	if free == 0 {
		mem := make([]byte, b.max)
		br := &Buffer{
			pool: b,
			Data: mem[:n],
		}
		return br
	}

	item := b.free[0]
	item.pool = b

	if free == 0 {
		b.free = b.free[:0]
		return item
	}

	b.free = b.free[1:]
	return item
}

// BitsBoot implements a custom pool for issue Buffers,
// but uses a internal []Buffer slice instead of the sync.Pool.
type BitsBoot struct{
	distance int
	pl       sync.Mutex
	pools    []*bitsBoot
	indexes  map[int]int
}

// NewBitsBoot returns a new instance of a BitsBoot which returns
// allocated and managed Buffer.
func NewBitsBoot(distance int, initialAmount int) *BitsBoot {
	initials := make([]*bitsBoot, 0)
	indexes := make(map[int]int)

	for i := 1; i <= initialAmount; i++ {
		sizeDist := distance * i

		indexes[sizeDist] = len(initials)
		initials = append(initials, &bitsBoot{
			max: sizeDist,
			free: make([]*Buffer, 0, 100),
		})
	}

	return &BitsBoot{
		indexes:  indexes,
		distance: distance,
		pools:    initials,
	}
}

// Put returns the []byte by using the capacity of the slice to find its pool.
func (bp *BitsBoot) Put(bu *Buffer) {
	bu.pool = nil

	bp.pl.Lock()
	index, ok := bp.indexes[bu.bit]
	if !ok {
		bp.pl.Unlock()
		return
	}
	pool := bp.pools[index]
	bp.pl.Unlock()

	pool.Put(bu)
}

// Get returns a new or existing []byte from it's internal size RangePool.
// It gets a RangePool or creates one if non exists for the size + it's distance value
// then gets a []byte from that RangePool.
func (bp *BitsBoot) Get(size int) *Buffer {
	bp.pl.Lock()

	if poolIndex, ok := bp.indexes[size]; ok {
		pool := bp.pools[poolIndex]
		bp.pl.Unlock()

		return pool.Get(size)
	}

	// loop through RangePool till we find the distance where size is no more
	// greater, which means that pool will be suitable as the size provider for
	// this size need.
	for _, pool := range bp.pools {
		if pool.max < size {
			continue
		}

		bp.pl.Unlock()
		return pool.Get(size)
	}

	// We dont have any pool within size range, so create new RangePool suited for this size.
	newDistance := ((size / bp.distance) + 1) * bp.distance
	newPool := &bitsBoot{
		max: newDistance,
		free: make([]*Buffer, 0, 100),
	}

	bp.indexes[newDistance] = len(bp.pools)
	bp.pools = append(bp.pools, newPool)
	bp.pl.Unlock()

	return newPool.Get(size)
}

//************************************************************************
// BitsBoot
//************************************************************************

type bitsPool struct {
	max  int
	pool *sync.Pool
	source bufferHandler
}

func (b *bitsPool) Put(bu *Buffer)  {
	bu.pool = nil
	b.pool.Put(bu)
}

func (b *bitsPool) Get(n int) *Buffer {
	br := b.pool.Get().(*Buffer)
	br.Data = br.Data[:n]
	br.pool = b
	return br
}

// BitsPool implements a custom pool for issue Buffers,
// using the sync.Pool.
type BitsPool struct{
	distance int
	pl       sync.Mutex
	pools    []*bitsPool
	indexes  map[int]int
}

// NewBitsPool returns a new instance of a BitsPool which returns
// allocated and managed Buffer.
func NewBitsPool(distance int, initialAmount int) *BitsPool {
	initials := make([]*bitsPool, 0)
	indexes := make(map[int]int)

	for i := 1; i <= initialAmount; i++ {
		sizeDist := distance * i

		indexes[sizeDist] = len(initials)
		initials = append(initials, &bitsPool{
			max: sizeDist,
			pool: &sync.Pool{
				New: func() interface{} {
					return &Buffer{
						bit: sizeDist,
						Data: make([]byte, sizeDist),
					}
				},
			},
		})
	}

	return &BitsPool{
		indexes:  indexes,
		distance: distance,
		pools:    initials,
	}
}

// Get returns a new or existing []byte from it's internal size RangePool.
// It gets a RangePool or creates one if non exists for the size + it's distance value
// then gets a []byte from that RangePool.
func (bp *BitsPool) Get(size int) *Buffer {
	bp.pl.Lock()
	defer bp.pl.Unlock()

	if poolIndex, ok := bp.indexes[size]; ok {
		pool := bp.pools[poolIndex]
		return pool.Get(size)
	}

	// loop through RangePool till we find the distance where size is no more
	// greater, which means that pool will be suitable as the size provider for
	// this size need.
	for _, pool := range bp.pools {
		if pool.max < size {
			continue
		}

		return pool.Get(size)
	}

	// We dont have any pool within size range, so create new RangePool suited for this size.
	newDistance := ((size / bp.distance) + 1) * bp.distance
	newPool := &bitsPool{
		max: newDistance,
		pool: &sync.Pool{
				New: func() interface{} {
					return &Buffer{
						bit: newDistance,
						Data: make([]byte, newDistance),
					}
			},
		},
	}

	bp.indexes[newDistance] = len(bp.pools)
	bp.pools = append(bp.pools, newPool)
	return newPool.Get(size)
}

//************************************************************************
// BytesPool
//************************************************************************

type rangePool struct {
	max  int
	pool *sync.Pool
}

// BytesPool exists to contain multiple RangePool that lies within giving distance range.
// It creates a internal array of BytesPool which are distanced between each other by
// provided distance. Whenever giving call to get a []byte for a giving size is
// within existing pool distances, it calls that RangePool responsible for that size and
// retrieves giving []byte from that pool. If no range as such exists, it creates
// a new RangePool for the size + BytesPool.Distance set an instantiation, then retrieves
// a []byte from that.
type BytesPool struct {
	distance int
	pl       sync.Mutex
	pools    []*rangePool
	indexes  map[int]int
}

// NewBytesPool returns a new instance of a BytesPool with size distance used for new pools
// and creates as many as the initialAmount of RangePools internally to service those size
// requests.
func NewBytesPool(distance int, initialAmount int) *BytesPool {
	initials := make([]*rangePool, 0)
	indexes := make(map[int]int)

	for i := 1; i <= initialAmount; i++ {
		sizeDist := distance * i

		indexes[sizeDist] = len(initials)
		initials = append(initials, &rangePool{
			max: sizeDist,
			pool: &sync.Pool{
				New: func() interface{} {
					return bytes.NewBuffer(make([]byte, 0, sizeDist))
				},
			},
		})
	}

	return &BytesPool{
		distance: distance,
		pools:    initials,
		indexes:  indexes,
	}
}

// Put returns the []byte by using the capacity of the slice to find its pool.
func (bp *BytesPool) Put(bu *bytes.Buffer) {
	bp.pl.Lock()
	defer bp.pl.Unlock()

	if index, ok := bp.indexes[bu.Cap()]; ok {
		pool := bp.pools[index]
		pool.pool.Put(bu)
	}
}

// Get returns a new or existing []byte from it's internal size RangePool.
// It gets a RangePool or creates one if non exists for the size + it's distance value
// then gets a []byte from that RangePool.
func (bp *BytesPool) Get(size int) *bytes.Buffer {
	bp.pl.Lock()
	defer bp.pl.Unlock()

	// loop through RangePool till we find the distance where size is no more
	// greater, which means that pool will be suitable as the size provider for
	// this size need.
	for _, pool := range bp.pools {
		if pool.max < size {
			continue
		}

		return pool.pool.Get().(*bytes.Buffer)
	}

	// We dont have any pool within size range, so create new RangePool suited for this size.
	newDistance := ((size / bp.distance) + 1) * bp.distance
	newPool := &rangePool{
		max: newDistance,
		pool: &sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 0, newDistance))
			},
		},
	}

	bp.indexes[newDistance] = len(bp.pools)
	bp.pools = append(bp.pools, newPool)

	return newPool.pool.Get().(*bytes.Buffer)
}

//************************************************************************
// BytePool
//************************************************************************

// BytePool implements a leaky pool of []byte in the form of a bounded
// channel.
type BytePool struct {
	c chan []byte
	w int
}

// NewBytePool creates a new BytePool bounded to the given maxSize, with new
// byte arrays sized based on width.
func NewBytePool(maxSize int, width int) (bp *BytePool) {
	return &BytePool{
		c: make(chan []byte, maxSize),
		w: width,
	}
}

// Get gets a []byte from the BytePool, or creates a new one if none are
// available in the pool.
func (bp *BytePool) Get() (b []byte) {
	select {
	case b = <-bp.c:
		// reuse existing buffer
	default:
		// create new buffer
		b = make([]byte, bp.w)
	}
	return
}

// Put returns the given Buffer to the BytePool.
func (bp *BytePool) Put(b []byte) {
	select {
	case bp.c <- b:
		// buffer went back into pool
	default:
		// buffer didn't go back into pool, just discard
	}
}

// Width returns the width of the byte arrays in this pool.
func (bp *BytePool) Width() (n int) {
	return bp.w
}
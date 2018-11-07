package pbytes_test

import (
	"math/rand"
	"testing"

	"github.com/influx6/faux/pools/pbytes"
)

func BenchmarkBitsBoot(b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()

	provider := pbytes.NewBitsBoot(128, 10)
	benchmarkPool(b, provider)
}

func BenchmarkBitsPool(b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()

	provider := pbytes.NewBitsPool(128, 10)
	benchmarkPool(b, provider)
}

type provider interface {
	Get(int) *pbytes.Buffer
}

func benchmarkPool(b *testing.B, handler provider) {
	b.Run("2bytes", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()

		b.StartTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buff := handler.Get(2)
				buff.Discard()
			}
		})
		b.StopTimer()
	})

	b.Run("4bytes", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		b.StartTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buff := handler.Get(4)
				buff.Discard()
			}
		})
		b.StopTimer()
	})

	b.Run("16bytes", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		b.StartTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buff := handler.Get(16)
				buff.Discard()
			}
		})
		b.StopTimer()
	})

	b.Run("32bytes", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		b.StartTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buff := handler.Get(32)
				buff.Discard()
			}
		})
		b.StopTimer()
	})

	b.Run("64bytes", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		b.StartTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buff := handler.Get(64)
				buff.Discard()
			}
		})
		b.StopTimer()
	})

	b.Run("128bytes", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			buff := handler.Get(128)
			buff.Discard()
		}
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buff := handler.Get(128)
				buff.Discard()
			}
		})
		b.StopTimer()
	})

	b.Run("512bytes", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		b.StartTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buff := handler.Get(512)
				buff.Discard()
			}
		})
		b.StopTimer()
	})

	b.Run("1024bytes", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		b.StartTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buff := handler.Get(1024)
				buff.Discard()
			}
		})
		b.StopTimer()
	})

	b.Run("4024bytes", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		b.StartTimer()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buff := handler.Get(1024 * 4)
				buff.Discard()
			}
		})
		b.StopTimer()
	})

	b.Run("8024bytes", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		b.StartTimer()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buff := handler.Get(1024 * 8)
				buff.Discard()
			}
		})
		b.StopTimer()
	})

	b.Run("16024bytes", func(b *testing.B) {
		b.ReportAllocs()
		b.StopTimer()
		b.StartTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				buff := handler.Get(1024 * 16)
				buff.Discard()
			}
		})
		b.StopTimer()
	})
}

var pub = "PUB "
var empty = []byte("")
var ch = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@$#%^&*()")

func sizedPayload(sz int) []byte {
	payload := make([]byte, len(pub)+sz)
	nx := copy(payload, pub)
	copy(payload[nx:], sizedBytes(sz))
	return payload
}

func sizedBytes(sz int) []byte {
	if sz <= 0 {
		return empty
	}

	b := make([]byte, sz)
	for i := range b {
		b[i] = ch[rand.Intn(len(ch))]
	}
	return b
}

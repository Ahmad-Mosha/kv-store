package engine

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

const benchmarkKeyCount = 1024

func benchmarkKeys(count int) [][]byte {
	keys := make([][]byte, count)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%012d", i))
	}
	return keys
}

func benchmarkLog(b *testing.B, records, keyCount, valueSize, deletions int) []byte {
	b.Helper()
	keys := benchmarkKeys(keyCount)
	value := bytes.Repeat([]byte{0x5a}, valueSize)
	var log bytes.Buffer
	log.Grow(fileHeaderSize + records*(recordHeaderSize+16+valueSize) + deletions*(recordHeaderSize+16))
	log.Write(encodeFileHeader())
	for i := range records {
		record, err := encodeRecord(kindSet, keys[i%keyCount], value)
		if err != nil {
			b.Fatal(err)
		}
		log.Write(record)
	}
	for i := range deletions {
		record, err := encodeRecord(kindDelete, keys[i], nil)
		if err != nil {
			b.Fatal(err)
		}
		log.Write(record)
	}
	return log.Bytes()
}

func writeBenchmarkLog(b *testing.B, dir string, log []byte) {
	b.Helper()
	if err := os.WriteFile(filepath.Join(dir, dataFileName), log, 0600); err != nil {
		b.Fatal(err)
	}
}

func openBenchmarkDB(b *testing.B, valueSize int) (*DB, [][]byte) {
	b.Helper()
	dir := b.TempDir()
	writeBenchmarkLog(b, dir, benchmarkLog(b, benchmarkKeyCount, benchmarkKeyCount, valueSize, 0))
	db, err := Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := db.Close(); err != nil {
			b.Error(err)
		}
	})
	return db, benchmarkKeys(benchmarkKeyCount)
}

func BenchmarkGet(b *testing.B) {
	for _, size := range []int{64, 1024, 64 << 10} {
		b.Run(fmt.Sprintf("Value%dB", size), func(b *testing.B) {
			db, keys := openBenchmarkDB(b, size)
			b.ReportAllocs()
			b.SetBytes(int64(size))
			i := 0
			for b.Loop() {
				value, err := db.Get(keys[i%len(keys)])
				if err != nil || len(value) != size || value[0] != 0x5a {
					b.Fatalf("Get: length=%d, error=%v", len(value), err)
				}
				i++
			}
		})
	}
}

func BenchmarkSet(b *testing.B) {
	for _, size := range []int{64, 1024, 64 << 10} {
		b.Run(fmt.Sprintf("Value%dB", size), func(b *testing.B) {
			db, keys := openBenchmarkDB(b, size)
			value := bytes.Repeat([]byte{0x5a}, size)
			b.ReportAllocs()
			b.SetBytes(int64(size))
			i := 0
			for b.Loop() {
				if err := db.Set(keys[i%len(keys)], value); err != nil {
					b.Fatal(err)
				}
				i++
			}
		})
	}
}

func BenchmarkConcurrent(b *testing.B) {
	for _, writes := range []bool{false, true} {
		name := "ReadOnly"
		if writes {
			name = "Read95Write5"
		}
		b.Run(name, func(b *testing.B) {
			db, keys := openBenchmarkDB(b, 1024)
			value := bytes.Repeat([]byte{0x5a}, 1024)
			var workers atomic.Uint64
			var firstErr error
			var errOnce sync.Once
			b.ReportAllocs()
			b.SetBytes(1024)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				worker := int(workers.Add(1) - 1)
				i := worker * 17
				for pb.Next() {
					key := keys[i%len(keys)]
					var err error
					if writes && i%20 == 0 {
						key = keys[(i/20*17+worker*257)%len(keys)]
						err = db.Set(key, value)
					} else {
						var got []byte
						got, err = db.Get(key)
						if err == nil && (len(got) != 1024 || got[0] != 0x5a) {
							err = fmt.Errorf("Get returned invalid value of length %d", len(got))
						}
					}
					if err != nil {
						errOnce.Do(func() { firstErr = err })
						return
					}
					i++
				}
			})
			b.StopTimer()
			if firstErr != nil {
				b.Fatal(firstErr)
			}
		})
	}
}

func BenchmarkFileAppend(b *testing.B) {
	for _, syncEach := range []bool{false, true} {
		name := "WriteOnly"
		if syncEach {
			name = "WriteAndSync"
		}
		b.Run(name, func(b *testing.B) {
			file, err := os.Create(filepath.Join(b.TempDir(), "append.log"))
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := file.Close(); err != nil {
					b.Error(err)
				}
			})
			record, err := encodeRecord(kindSet, benchmarkKeys(1)[0], bytes.Repeat([]byte{0x5a}, 1024))
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(1024)
			written := 0
			for b.Loop() {
				if written+len(record) > 64<<20 {
					b.StopTimer()
					if err := file.Truncate(0); err != nil {
						b.Fatal(err)
					}
					if _, err := file.Seek(0, io.SeekStart); err != nil {
						b.Fatal(err)
					}
					written = 0
					b.StartTimer()
				}
				n, err := file.Write(record)
				if err != nil || n != len(record) {
					b.Fatalf("Write: bytes=%d, error=%v", n, err)
				}
				if syncEach {
					if err := file.Sync(); err != nil {
						b.Fatal(err)
					}
				}
				written += n
			}
		})
	}
}

func BenchmarkRecovery(b *testing.B) {
	for _, records := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("Records%d", records), func(b *testing.B) {
			dir := b.TempDir()
			keyCount := min(benchmarkKeyCount, records)
			log := benchmarkLog(b, records, keyCount, 256, 0)
			writeBenchmarkLog(b, dir, log)
			db, err := Open(dir)
			if err != nil {
				b.Fatal(err)
			}
			if err := db.Close(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(log)))
			for b.Loop() {
				db, err := Open(dir)
				if err != nil {
					b.Fatal(err)
				}
				valid := len(db.index) == keyCount && db.offset == int64(len(log))
				err = db.Close()
				if err != nil || !valid {
					b.Fatalf("Open/Close: valid=%t, error=%v", valid, err)
				}
			}
		})
	}
}

func BenchmarkCompaction(b *testing.B) {
	b.StopTimer()
	dir := b.TempDir()
	log := benchmarkLog(b, 10000, 1000, 256, 500)
	const compactedSize = fileHeaderSize + 500*(recordHeaderSize+16+256)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		writeBenchmarkLog(b, dir, log)
		db, err := Open(dir)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		err = db.Compact()
		b.StopTimer()
		if err != nil {
			db.Close()
			b.Fatal(err)
		}
		valid := len(db.index) == 500 && db.offset == compactedSize
		info, err := db.file.Stat()
		closeErr := db.Close()
		if err != nil || closeErr != nil || !valid || info.Size() != compactedSize {
			b.Fatalf("Compact: valid=%t, stat=%v, close=%v", valid, err, closeErr)
		}
	}
	b.ReportMetric(float64(len(log)), "before-B/op")
	b.ReportMetric(compactedSize, "after-B/op")
}

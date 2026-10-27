package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Ahmad-Mosha/kv-store/engine"
	"github.com/Ahmad-Mosha/kv-store/internal/protocol"
)

func BenchmarkTCP(b *testing.B) {
	b.StopTimer()
	value := bytes.Repeat([]byte{'v'}, 1024)
	keys := make([][]byte, 1024)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "%016d", i)
	}
	fixture := benchmarkTCPFixture(b, keys, value)
	for _, workload := range []struct {
		name  string
		mixed bool
	}{{"Get", false}, {"Mixed95Read5Write", true}} {
		b.Run(workload.name, func(b *testing.B) {
			for _, clients := range []int{1, 4} {
				b.Run(fmt.Sprintf("Clients%d", clients), func(b *testing.B) {
					benchmarkTCP(b, clients, workload.mixed, keys, value, fixture)
				})
			}
		})
	}
}

func benchmarkTCPFixture(b *testing.B, keys [][]byte, value []byte) []byte {
	dir := b.TempDir()
	db, err := engine.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	for _, key := range keys {
		if err := db.Set(key, value); err != nil {
			db.Close()
			b.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "data.log"))
	if err != nil {
		b.Fatal(err)
	}
	return data
}

func benchmarkTCP(b *testing.B, clients int, mixed bool, keys [][]byte, value, fixture []byte) {
	b.StopTimer()
	dir := b.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "data.log"), fixture, 0600); err != nil {
		b.Fatal(err)
	}
	db, err := engine.Open(dir)
	if err != nil {
		b.Fatal(err)
	}
	safeToClose := true
	b.Cleanup(func() {
		if safeToClose {
			if err := db.Close(); err != nil {
				b.Error(err)
			}
		}
	})
	bucket, err := NewBucket(1_000_000, 1e9)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(bucket.Stop)
	config := Config{MaxConnections: clients, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second}
	srv, err := New(db, bucket, config)
	if err != nil {
		b.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve(listener) }()
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			safeToClose = false
			b.Error(err)
			return
		}
		select {
		case err := <-serveDone:
			if !errors.Is(err, ErrServerClosed) {
				b.Errorf("Serve: %v", err)
			}
		case <-ctx.Done():
			b.Error("Serve did not exit after shutdown")
		}
	})

	connections := make([]net.Conn, clients)
	readers := make([]*protocol.Reader, clients)
	for i := range connections {
		conn, err := net.DialTimeout("tcp", listener.Addr().String(), 5*time.Second)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { conn.Close() })
		connections[i], readers[i] = conn, protocol.NewReader(conn)
		if err := benchmarkRoundTrip(conn, readers[i], protocol.Request{Op: "GET", Key: keys[i]}, value); err != nil {
			b.Fatal(err)
		}
	}

	start := make(chan struct{})
	results := make(chan error, clients)
	var ready sync.WaitGroup
	ready.Add(clients)
	for worker := range clients {
		go func() {
			ready.Done()
			<-start
			for i := b.N * worker / clients; i < b.N*(worker+1)/clients; i++ {
				request := protocol.Request{Op: "GET", Key: keys[(i*17+worker*257)%len(keys)]}
				if mixed && i%20 == 0 {
					request.Op, request.Value = "SET", value
					request.Key = keys[(i/20*17+worker*257)%len(keys)]
				}
				if err := benchmarkRoundTrip(connections[worker], readers[worker], request, value); err != nil {
					results <- err
					return
				}
			}
			results <- nil
		}()
	}
	ready.Wait()
	b.ReportAllocs()
	b.SetBytes(int64(len(value)))
	b.ResetTimer()
	b.StartTimer()
	close(start)
	var firstErr error
	for range clients {
		if err := <-results; firstErr == nil {
			firstErr = err
		}
	}
	b.StopTimer()
	if firstErr != nil {
		b.Fatal(firstErr)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "requests/s")
}

func benchmarkRoundTrip(conn net.Conn, reader *protocol.Reader, request protocol.Request, value []byte) error {
	if err := conn.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return err
	}
	if err := protocol.WriteRequest(conn, request); err != nil {
		return err
	}
	response, err := reader.ReadResponse()
	if err != nil {
		return err
	}
	if !response.OK {
		return fmt.Errorf("%s failed: %+v", request.Op, response.Error)
	}
	if request.Op == "GET" && (response.Value == nil || !bytes.Equal(*response.Value, value)) {
		return errors.New("GET returned an unexpected value")
	}
	return nil
}

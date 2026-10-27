package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Ahmad-Mosha/kv-store/engine"
	"github.com/Ahmad-Mosha/kv-store/internal/protocol"
	"github.com/Ahmad-Mosha/kv-store/internal/server"
	"github.com/Ahmad-Mosha/rate-limiter/limiter"
)

func TestInvalidConfigurationHasNoFilesystemSideEffects(t *testing.T) {
	for _, args := range [][]string{
		{"-max-connections", "0"}, {"-max-connections", "4097"},
		{"-read-timeout", "0"}, {"-write-timeout", "25h"},
		{"-shutdown-timeout", "0"}, {"-shutdown-timeout", "25h"},
		{"-burst", "0"}, {"-burst", "1000001"},
		{"-rate", "0"}, {"-rate", "NaN"}, {"-rate", "+Inf"}, {"-rate", "0.000001"},
		{"-addr", ""}, {"-addr", "127.0.0.1:invalid"}, {"unexpected"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "not-created")
			err := run(context.Background(), append([]string{"-data", dir}, args...), io.Discard)
			if err == nil {
				t.Fatal("expected configuration error")
			}
			if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("data directory was created: %v", err)
			}
		})
	}
	if _, err := parseOptions([]string{"-data", ""}, io.Discard); err == nil {
		t.Fatal("accepted empty data directory")
	}
	if err := run(context.Background(), []string{"-h"}, io.Discard); err != flag.ErrHelp {
		t.Fatalf("help = %v", err)
	}
}

func TestCanceledRunDoesNotCreateDataDirectory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dir := filepath.Join(t.TempDir(), "not-created")
	if err := run(ctx, []string{"-data", dir}, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v", err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("data directory was created: %v", err)
	}
}

type addressWriter chan string

func (w addressWriter) Write(p []byte) (int, error) {
	w <- strings.TrimSpace(strings.TrimPrefix(string(p), "listening on "))
	return len(p), nil
}

func TestRunPersistsAndReleasesDatabaseOnShutdown(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addresses := make(addressWriter, 1)
	done := make(chan error, 1)
	go func() { done <- run(ctx, []string{"-addr", "127.0.0.1:0", "-data", dir}, addresses) }()
	var addr string
	select {
	case addr = <-addresses:
	case err := <-done:
		t.Fatalf("server exited before listening: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("server did not start")
	}
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if err := protocol.WriteRequest(conn, protocol.Request{Op: "SET", Key: []byte("city"), Value: []byte("Cairo")}); err != nil {
		t.Fatal(err)
	}
	response, err := protocol.NewReader(conn).ReadResponse()
	if err != nil || !response.OK {
		t.Fatalf("SET = %#v, %v", response, err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not finish")
	}
	db, err := engine.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	value, err := db.Get([]byte("city"))
	if err != nil || !bytes.Equal(value, []byte("Cairo")) {
		t.Fatalf("recovered value = %q, %v", value, err)
	}
}

func TestListenFailureReleasesDatabase(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	dir := t.TempDir()
	if err := run(context.Background(), []string{"-addr", listener.Addr().String(), "-data", dir}, io.Discard); err == nil {
		t.Fatal("expected listen failure")
	}
	db, err := engine.Open(dir)
	if err != nil {
		t.Fatalf("database lock was not released: %v", err)
	}
	db.Close()
}

type blockingStore struct {
	started chan struct{}
	release chan struct{}
}

func (s *blockingStore) Set(_, _ []byte) error     { close(s.started); <-s.release; return nil }
func (*blockingStore) Get([]byte) ([]byte, error)  { return nil, engine.ErrNotFound }
func (*blockingStore) Delete([]byte) error         { return nil }
func (*blockingStore) Exists([]byte) (bool, error) { return false, nil }

type allowLimiter struct{}

func (allowLimiter) Allow(context.Context, string) (limiter.Result, error) {
	return limiter.Result{Allowed: true}, nil
}

func TestServeShutdownDeadlineDoesNotWaitForBlockedStorage(t *testing.T) {
	store := &blockingStore{started: make(chan struct{}), release: make(chan struct{})}
	srv, err := server.New(store, allowLimiter{}, server.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Cleanup(func() {
		close(store.release)
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer drainCancel()
		if err := srv.Shutdown(drainCtx); err != nil {
			t.Error(err)
		}
	})
	type result struct {
		safeToClose bool
		err         error
	}
	done := make(chan result, 1)
	go func() { safe, err := serve(ctx, srv, listener, 30*time.Millisecond); done <- result{safe, err} }()
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := protocol.WriteRequest(conn, protocol.Request{Op: "SET", Key: []byte("key")}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-store.started:
	case <-time.After(5 * time.Second):
		t.Fatal("storage operation did not start")
	}
	cancel()
	select {
	case result := <-done:
		if result.safeToClose || !errors.Is(result.err, context.DeadlineExceeded) {
			t.Fatalf("serve = %v, %v; expected incomplete shutdown", result.safeToClose, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve waited for blocked storage")
	}
}

func TestServeAlreadyCanceledContextClosesListener(t *testing.T) {
	srv, err := server.New(&blockingStore{}, allowLimiter{}, server.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	safe, err := serve(ctx, srv, listener, time.Second)
	if !safe || err != nil {
		t.Fatalf("serve = %v, %v", safe, err)
	}
	if conn, err := net.DialTimeout("tcp", listener.Addr().String(), 100*time.Millisecond); err == nil {
		conn.Close()
		t.Fatal("listener remained open")
	}
}

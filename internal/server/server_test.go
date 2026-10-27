package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Ahmad-Mosha/kv-store/engine"
	"github.com/Ahmad-Mosha/kv-store/internal/protocol"
	"github.com/Ahmad-Mosha/rate-limiter/limiter"
)

type memoryStore struct {
	mu     sync.Mutex
	values map[string][]byte
	calls  []string
	hook   func(string, []byte) error
}

func newMemoryStore() *memoryStore { return &memoryStore{values: make(map[string][]byte)} }

func (s *memoryStore) before(op string, key []byte) error {
	s.mu.Lock()
	s.calls = append(s.calls, op+":"+string(key))
	s.mu.Unlock()
	if s.hook != nil {
		return s.hook(op, key)
	}
	return nil
}

func (s *memoryStore) Set(key, value []byte) error {
	if err := s.before("SET", key); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[string(key)] = bytes.Clone(value)
	return nil
}

func (s *memoryStore) Get(key []byte) ([]byte, error) {
	if err := s.before("GET", key); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	value, exists := s.values[string(key)]
	if !exists {
		return nil, engine.ErrNotFound
	}
	return bytes.Clone(value), nil
}

func (s *memoryStore) Delete(key []byte) error {
	if err := s.before("DELETE", key); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.values, string(key))
	return nil
}

func (s *memoryStore) Exists(key []byte) (bool, error) {
	if err := s.before("EXISTS", key); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.values[string(key)]
	return exists, nil
}

func (s *memoryStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

type recordingLimiter struct {
	mu   sync.Mutex
	keys []string
	hook func(context.Context, string) (limiter.Result, error)
}

func (r *recordingLimiter) Allow(ctx context.Context, key string) (limiter.Result, error) {
	r.mu.Lock()
	r.keys = append(r.keys, key)
	r.mu.Unlock()
	if r.hook != nil {
		return r.hook(ctx, key)
	}
	return limiter.Result{Allowed: true}, nil
}

func (r *recordingLimiter) calls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.keys...)
}

type runningServer struct {
	server *Server
	addr   string
	done   chan error
}

func startTestServer(t *testing.T, store Store, rl limiter.RateLimiter, cfg Config) runningServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return startServerOnListener(t, store, rl, cfg, listener)
}

func startServerOnListener(t *testing.T, store Store, rl limiter.RateLimiter, cfg Config, listener net.Listener) runningServer {
	t.Helper()
	s, err := New(store, rl, cfg)
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	running := runningServer{server: s, addr: listener.Addr().String(), done: make(chan error, 1)}
	go func() { running.done <- s.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
		select {
		case err := <-running.done:
			if !errors.Is(err, ErrServerClosed) {
				t.Errorf("Serve after shutdown = %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("Serve did not exit after shutdown")
		}
	})
	return running
}

type wireClient struct {
	conn   net.Conn
	reader *bufio.Reader
}

func dialTestClient(t *testing.T, addr string) *wireClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return &wireClient{conn: conn, reader: bufio.NewReader(conn)}
}

func requestBytes(t *testing.T, req protocol.Request) []byte {
	t.Helper()
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	return append(data, '\n')
}

func (c *wireClient) send(t *testing.T, req protocol.Request) protocol.Response {
	t.Helper()
	if _, err := c.conn.Write(requestBytes(t, req)); err != nil {
		t.Fatal(err)
	}
	return c.response(t)
}

func (c *wireClient) response(t *testing.T) protocol.Response {
	t.Helper()
	data, err := c.reader.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	var response protocol.Response
	if err := json.Unmarshal(data, &response); err != nil {
		t.Fatalf("decode response %q: %v", data, err)
	}
	return response
}

func requireOK(t *testing.T, response protocol.Response) {
	t.Helper()
	if !response.OK || response.Error != nil {
		t.Fatalf("response = %+v; want success", response)
	}
}

func requireCode(t *testing.T, response protocol.Response, code string) {
	t.Helper()
	if response.OK || response.Error == nil || response.Error.Code != code || response.Value != nil || response.Exists != nil {
		t.Fatalf("response = %+v; want error %q with no success fields", response, code)
	}
}

func TestTCPCRUDPersistsAcrossEngineAndServerRestart(t *testing.T) {
	dir := t.TempDir()
	db, err := engine.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	running := startTestServer(t, db, &recordingLimiter{}, DefaultConfig())
	client := dialTestClient(t, running.addr)
	key, value := []byte{0xff, 0, '\n'}, []byte{0, '\n', 0xff, '\r'}
	requireOK(t, client.send(t, protocol.Request{Op: "SET", Key: key, Value: value}))
	got := client.send(t, protocol.Request{Op: "GET", Key: key})
	requireOK(t, got)
	if got.Value == nil || !bytes.Equal(*got.Value, value) {
		t.Fatalf("GET value = %v", got.Value)
	}
	got = client.send(t, protocol.Request{Op: "EXISTS", Key: key})
	requireOK(t, got)
	if got.Exists == nil || !*got.Exists {
		t.Fatal("EXISTS did not report key")
	}
	requireOK(t, client.send(t, protocol.Request{Op: "SET", Key: []byte("empty")}))
	got = client.send(t, protocol.Request{Op: "GET", Key: []byte("empty")})
	requireOK(t, got)
	if got.Value == nil || len(*got.Value) != 0 {
		t.Fatal("empty GET value was omitted or changed")
	}
	requireOK(t, client.send(t, protocol.Request{Op: "SET", Key: []byte("deleted"), Value: []byte("obsolete")}))
	requireOK(t, client.send(t, protocol.Request{Op: "DELETE", Key: []byte("deleted")}))
	requireCode(t, client.send(t, protocol.Request{Op: "GET", Key: []byte("deleted")}), "NOT_FOUND")
	if err := running.server.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = engine.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	running = startTestServer(t, db, &recordingLimiter{}, DefaultConfig())
	client = dialTestClient(t, running.addr)
	got = client.send(t, protocol.Request{Op: "GET", Key: key})
	requireOK(t, got)
	if got.Value == nil || !bytes.Equal(*got.Value, value) {
		t.Fatal("value did not survive restart")
	}
	requireCode(t, client.send(t, protocol.Request{Op: "GET", Key: []byte("deleted")}), "NOT_FOUND")
}

func TestTCPFragmentedAndCoalescedFrames(t *testing.T) {
	store := newMemoryStore()
	running := startTestServer(t, store, &recordingLimiter{}, DefaultConfig())
	client := dialTestClient(t, running.addr)
	key, value := []byte{0, 0xff}, []byte{0, 1, 0xff, '\n'}
	frame := requestBytes(t, protocol.Request{Op: "SET", Key: key, Value: value})
	for _, chunk := range [][]byte{frame[:1], frame[1:7], frame[7 : len(frame)-1], frame[len(frame)-1:]} {
		if _, err := client.conn.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	requireOK(t, client.response(t))
	batch := requestBytes(t, protocol.Request{Op: "GET", Key: key})
	batch = append(batch, requestBytes(t, protocol.Request{Op: "DELETE", Key: key})...)
	batch = append(batch, requestBytes(t, protocol.Request{Op: "EXISTS", Key: key})...)
	if _, err := client.conn.Write(batch); err != nil {
		t.Fatal(err)
	}
	got := client.response(t)
	requireOK(t, got)
	if got.Value == nil || !bytes.Equal(*got.Value, value) {
		t.Fatal("fragmented payload changed")
	}
	requireOK(t, client.response(t))
	got = client.response(t)
	requireOK(t, got)
	if got.Exists == nil || *got.Exists {
		t.Fatal("coalesced commands executed out of order")
	}
}

func TestTCPMalformedAndOversizedFramesCloseConnection(t *testing.T) {
	for _, scenario := range []struct {
		name, code string
		frame      []byte
	}{
		{"malformed", "BAD_REQUEST", []byte("{not json}\n")},
		{"duplicate-field", "BAD_REQUEST", []byte("{\"op\":\"GET\",\"op\":\"SET\",\"key\":\"aw==\"}\n")},
		{"oversized", "FRAME_TOO_LARGE", append(bytes.Repeat([]byte{' '}, 2<<20+1), '\n')},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			store, rl := newMemoryStore(), &recordingLimiter{}
			running := startTestServer(t, store, rl, DefaultConfig())
			client := dialTestClient(t, running.addr)
			client.conn.Write(scenario.frame)
			requireCode(t, client.response(t), scenario.code)
			if _, err := client.reader.ReadByte(); err == nil {
				t.Fatal("malformed frame connection remained open")
			} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
				t.Fatal("server did not close malformed connection")
			}
			if store.callCount() != 0 || len(rl.calls()) != 0 {
				t.Fatal("malformed frame reached limiter/store")
			}
		})
	}
}

func TestTCPInvalidCommandsConsumeAdmissionBudgetWithoutMutation(t *testing.T) {
	store, rl := newMemoryStore(), &recordingLimiter{}
	running := startTestServer(t, store, rl, DefaultConfig())
	client := dialTestClient(t, running.addr)
	for _, req := range []protocol.Request{{Op: "UNKNOWN", Key: []byte("key")}, {Op: "GET"}} {
		requireCode(t, client.send(t, req), "BAD_REQUEST")
	}
	requireOK(t, client.send(t, protocol.Request{Op: "SET", Key: []byte("key"), Value: []byte("value")}))
	if store.callCount() != 1 || len(rl.calls()) != 3 {
		t.Fatalf("store/limiter calls = %d/%d", store.callCount(), len(rl.calls()))
	}
}

func TestTCPRateLimitSharedAcrossConnectionsUsingExistingModule(t *testing.T) {
	bucket, err := NewBucket(1, 1.0/3600)
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Stop()
	store := newMemoryStore()
	running := startTestServer(t, store, bucket, DefaultConfig())
	first, second := dialTestClient(t, running.addr), dialTestClient(t, running.addr)
	requireOK(t, first.send(t, protocol.Request{Op: "SET", Key: []byte("key"), Value: []byte("kept")}))
	denied := second.send(t, protocol.Request{Op: "DELETE", Key: []byte("key")})
	requireCode(t, denied, "RATE_LIMITED")
	if denied.Error.RetryAfterMS <= 0 {
		t.Fatal("denial omitted retry information")
	}
	first.conn.Close()
	third := dialTestClient(t, running.addr)
	requireCode(t, third.send(t, protocol.Request{Op: "GET", Key: []byte("key")}), "RATE_LIMITED")
	if store.callCount() != 1 {
		t.Fatal("rate-limited command reached storage")
	}
}

func TestTCPPipelinedDenialConsumesEntirePayload(t *testing.T) {
	store := newMemoryStore()
	store.values["kept"] = []byte("value")
	var calls int
	rl := &recordingLimiter{hook: func(context.Context, string) (limiter.Result, error) {
		calls++
		return limiter.Result{Allowed: calls > 1, RetryAfter: time.Nanosecond}, nil
	}}
	running := startTestServer(t, store, rl, DefaultConfig())
	client := dialTestClient(t, running.addr)
	batch := requestBytes(t, protocol.Request{Op: "SET", Key: []byte("denied"), Value: bytes.Repeat([]byte{0xff}, 4096)})
	batch = append(batch, requestBytes(t, protocol.Request{Op: "GET", Key: []byte("kept")})...)
	if _, err := client.conn.Write(batch); err != nil {
		t.Fatal(err)
	}
	denied := client.response(t)
	requireCode(t, denied, "RATE_LIMITED")
	if denied.Error.RetryAfterMS != 1 {
		t.Fatalf("submillisecond retry = %d; want 1", denied.Error.RetryAfterMS)
	}
	got := client.response(t)
	requireOK(t, got)
	if got.Value == nil || !bytes.Equal(*got.Value, []byte("value")) || store.callCount() != 1 {
		t.Fatal("denied payload corrupted next frame or reached storage")
	}
}

func TestTCPBackendErrorsAreFailClosedAndSanitized(t *testing.T) {
	for _, scenario := range []struct {
		name, code string
		err        error
		limiter    bool
	}{
		{"limiter", "UNAVAILABLE", errors.New("private/redis.sock password=secret"), true},
		{"missing", "NOT_FOUND", engine.ErrNotFound, false},
		{"closed", "UNAVAILABLE", engine.ErrClosed, false},
		{"faulted", "UNAVAILABLE", fmt.Errorf("private/data.log: %w", engine.ErrFaulted), false},
		{"bad-key", "BAD_REQUEST", engine.ErrInvalidKey, false},
		{"bad-value", "BAD_REQUEST", engine.ErrValueTooLarge, false},
		{"unexpected", "INTERNAL_ERROR", errors.New("private/data.log password=secret"), false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			store, rl := newMemoryStore(), &recordingLimiter{}
			if scenario.limiter {
				rl.hook = func(context.Context, string) (limiter.Result, error) { return limiter.Result{}, scenario.err }
			} else {
				store.hook = func(string, []byte) error { return scenario.err }
			}
			running := startTestServer(t, store, rl, DefaultConfig())
			response := dialTestClient(t, running.addr).send(t, protocol.Request{Op: "GET", Key: []byte("key")})
			requireCode(t, response, scenario.code)
			if strings.Contains(response.Error.Message, "private") || strings.Contains(response.Error.Message, "secret") {
				t.Fatalf("response leaked backend detail: %s", response.Error.Message)
			}
			if scenario.limiter && store.callCount() != 0 {
				t.Fatal("limiter error allowed storage execution")
			}
		})
	}
}

func TestTCPConcurrentClientsAndPerConnectionOrder(t *testing.T) {
	store, rl := newMemoryStore(), &recordingLimiter{}
	running := startTestServer(t, store, rl, DefaultConfig())
	results := make(chan error, 8)
	for worker := range 8 {
		client := dialTestClient(t, running.addr)
		go func() {
			reader := protocol.NewReader(client.conn)
			key := []byte(fmt.Sprintf("worker:%d", worker))
			for revision := range 8 {
				value := []byte(fmt.Sprintf("revision:%d", revision))
				for _, request := range []protocol.Request{{Op: "SET", Key: key, Value: value}, {Op: "GET", Key: key}} {
					if err := protocol.WriteRequest(client.conn, request); err != nil {
						results <- err
						return
					}
					response, err := reader.ReadResponse()
					if err != nil {
						results <- err
						return
					}
					if !response.OK || request.Op == "GET" && (response.Value == nil || !bytes.Equal(*response.Value, value)) {
						results <- fmt.Errorf("client %d: %s response = %+v", worker, request.Op, response)
						return
					}
				}
			}
			results <- nil
		}()
	}
	for range 8 {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}
	for _, key := range rl.calls() {
		if key != "127.0.0.1" {
			t.Fatalf("limiter key includes source port or client data: %q", key)
		}
	}
}

func TestServerConfigurationValidation(t *testing.T) {
	defaults := DefaultConfig()
	if defaults.MaxConnections != 128 || defaults.ReadTimeout != 30*time.Second || defaults.WriteTimeout != 5*time.Second {
		t.Fatalf("unexpected defaults: %+v", defaults)
	}
	if _, err := New(nil, &recordingLimiter{}, defaults); err == nil {
		t.Error("New accepted missing storage")
	}
	if _, err := New(newMemoryStore(), nil, defaults); err == nil {
		t.Error("New accepted missing rate limiter")
	}
	for _, bad := range []Config{
		{0, time.Second, time.Second}, {4097, time.Second, time.Second}, {-1, time.Second, time.Second},
		{1, 0, time.Second}, {1, -1, time.Second}, {1, 24*time.Hour + 1, time.Second},
		{1, time.Second, 0}, {1, time.Second, -1}, {1, time.Second, 24*time.Hour + 1},
	} {
		if _, err := New(newMemoryStore(), &recordingLimiter{}, bad); err == nil {
			t.Errorf("New accepted invalid config %+v", bad)
		}
	}
	for _, cfg := range []Config{{1, time.Nanosecond, time.Nanosecond}, {4096, 24 * time.Hour, 24 * time.Hour}} {
		if _, err := New(newMemoryStore(), &recordingLimiter{}, cfg); err != nil {
			t.Errorf("New rejected boundary config %+v: %v", cfg, err)
		}
	}
}

func TestBucketConfigurationValidation(t *testing.T) {
	for _, cfg := range []struct {
		burst int
		rate  float64
	}{
		{0, 1}, {-1, 1}, {1000001, 1}, {1, 0}, {1, -1}, {1, math.NaN()},
		{1, math.Inf(1)}, {1, math.Inf(-1)}, {1, 1e-300}, {1, 1.0 / 86401},
	} {
		if bucket, err := NewBucket(cfg.burst, cfg.rate); err == nil {
			bucket.Stop()
			t.Errorf("accepted invalid bucket %+v", cfg)
		}
	}
	for _, cfg := range []struct {
		burst int
		rate  float64
	}{{1, 1.0 / 86400}, {1000000, 1000000}, {1, 1e300}} {
		bucket, err := NewBucket(cfg.burst, cfg.rate)
		if err != nil {
			t.Fatalf("valid bucket %+v: %v", cfg, err)
		}
		bucket.Stop()
	}
}

type testAddr string

func (a testAddr) Network() string { return "tcp" }
func (a testAddr) String() string  { return string(a) }

func TestPeerKeyCanonicalization(t *testing.T) {
	for _, tc := range []struct{ addr, want string }{
		{"192.0.2.1:12345", "192.0.2.1"},
		{"[::ffff:192.0.2.1]:12345", "192.0.2.1"},
		{"[2001:0db8:0000::1]:321", "2001:db8::1"},
		{"[::1]:443", "::1"}, {"proxy.example:80", "proxy.example"}, {"pipe", "pipe"},
	} {
		if got := peerKey(testAddr(tc.addr)); got != tc.want {
			t.Errorf("peerKey(%q) = %q; want %q", tc.addr, got, tc.want)
		}
	}
}

func assertConnectionClosed(t *testing.T, conn net.Conn) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(time.Second))
	var one [1]byte
	if _, err := conn.Read(one[:]); err == nil {
		t.Fatal("connection remained open")
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("server did not close connection")
	}
}

func TestConnectionCapacityAndIdleReadDeadline(t *testing.T) {
	t.Run("connection-cap", func(t *testing.T) {
		store := newMemoryStore()
		store.values["key"] = []byte("value")
		cfg := DefaultConfig()
		cfg.MaxConnections = 1
		running := startTestServer(t, store, &recordingLimiter{}, cfg)
		first := dialTestClient(t, running.addr)
		requireOK(t, first.send(t, protocol.Request{Op: "GET", Key: []byte("key")}))
		second := dialTestClient(t, running.addr)
		assertConnectionClosed(t, second.conn)
		requireOK(t, first.send(t, protocol.Request{Op: "GET", Key: []byte("key")}))
		if store.callCount() != 2 {
			t.Fatal("over-capacity connection reached storage")
		}
	})
	t.Run("idle-read-timeout", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ReadTimeout = 30 * time.Millisecond
		store := newMemoryStore()
		running := startTestServer(t, store, &recordingLimiter{}, cfg)
		client := dialTestClient(t, running.addr)
		assertConnectionClosed(t, client.conn)
		if store.callCount() != 0 {
			t.Fatal("idle connection reached storage")
		}
	})
}

func TestTCPStorageSizeBoundaries(t *testing.T) {
	store, rl := newMemoryStore(), &recordingLimiter{}
	running := startTestServer(t, store, rl, DefaultConfig())
	client := dialTestClient(t, running.addr)
	key, value := bytes.Repeat([]byte{0xff}, engine.MaxKeySize), bytes.Repeat([]byte{0xff}, engine.MaxValueSize)
	requireOK(t, client.send(t, protocol.Request{Op: "SET", Key: key, Value: value}))
	got := client.send(t, protocol.Request{Op: "GET", Key: key})
	requireOK(t, got)
	if got.Value == nil || !bytes.Equal(*got.Value, value) {
		t.Fatal("maximum-sized value changed")
	}
	for _, req := range []protocol.Request{
		{Op: "SET", Key: append(bytes.Clone(key), 'x')},
		{Op: "SET", Key: []byte("key"), Value: append(bytes.Clone(value), 'x')},
		{Op: "GET", Key: []byte("key"), Value: []byte("unexpected")},
	} {
		requireCode(t, client.send(t, req), protocol.CodeBadRequest)
	}
	if store.callCount() != 2 || len(rl.calls()) != 5 {
		t.Fatalf("store/limiter calls = %d/%d; want 2/5", store.callCount(), len(rl.calls()))
	}
}

func waitForClosing(t *testing.T, s *Server) {
	t.Helper()
	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		closing := s.closing
		s.mu.Unlock()
		if closing {
			return
		}
		select {
		case <-deadline:
			t.Fatal("shutdown did not start")
		case <-ticker.C:
		}
	}
}

func TestShutdownClosesPartialFramesAndDrainsOnlyAdmittedCommand(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	store := newMemoryStore()
	store.hook = func(string, []byte) error {
		close(entered)
		<-release
		return nil
	}
	running := startTestServer(t, store, &recordingLimiter{}, DefaultConfig())
	idle := dialTestClient(t, running.addr)
	if _, err := idle.conn.Write([]byte(`{"op":"SET"`)); err != nil {
		t.Fatal(err)
	}
	active := dialTestClient(t, running.addr)
	batch := requestBytes(t, protocol.Request{Op: "SET", Key: []byte("first"), Value: []byte("kept")})
	batch = append(batch, requestBytes(t, protocol.Request{Op: "SET", Key: []byte("second"), Value: []byte("rejected")})...)
	if _, err := active.conn.Write(batch); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("command was not admitted")
	}
	done := make(chan error, 1)
	go func() { done <- running.server.Shutdown(context.Background()) }()
	waitForClosing(t, running.server)
	assertConnectionClosed(t, idle.conn)
	select {
	case err := <-done:
		t.Fatalf("shutdown returned before admitted command completed: %v", err)
	default:
	}
	once.Do(func() { close(release) })
	requireOK(t, active.response(t))
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish after response")
	}
	assertConnectionClosed(t, active.conn)
	if store.callCount() != 1 {
		t.Fatalf("pipelined command admitted after shutdown: %d storage calls", store.callCount())
	}
}

func TestShutdownDeadlineDoesNotWaitForStorageIO(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	store := newMemoryStore()
	store.hook = func(string, []byte) error {
		close(entered)
		<-release
		return nil
	}
	running := startTestServer(t, store, &recordingLimiter{}, DefaultConfig())
	client := dialTestClient(t, running.addr)
	if _, err := client.conn.Write(requestBytes(t, protocol.Request{Op: "SET", Key: []byte("key"), Value: []byte("value")})); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("command was not admitted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := running.server.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v; want deadline exceeded", err)
	}
	assertConnectionClosed(t, client.conn)
	once.Do(func() { close(release) })
	drainCtx, drainCancel := context.WithTimeout(context.Background(), time.Second)
	defer drainCancel()
	if err := running.server.Shutdown(drainCtx); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	value := bytes.Clone(store.values["key"])
	store.mu.Unlock()
	if !bytes.Equal(value, []byte("value")) {
		t.Fatal("test did not exercise a mutation completing after connection closure")
	}
}

func TestShutdownDeadlineCancelsLimiterContext(t *testing.T) {
	entered := make(chan struct{})
	rl := &recordingLimiter{hook: func(ctx context.Context, _ string) (limiter.Result, error) {
		close(entered)
		<-ctx.Done()
		return limiter.Result{}, ctx.Err()
	}}
	store := newMemoryStore()
	running := startTestServer(t, store, rl, DefaultConfig())
	client := dialTestClient(t, running.addr)
	if _, err := client.conn.Write(requestBytes(t, protocol.Request{Op: "SET", Key: []byte("key")})); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("limiter was not entered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := running.server.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v; want deadline exceeded", err)
	}
	if store.callCount() != 0 {
		t.Fatal("canceled admission reached storage")
	}
}

type pipeListener struct {
	conns  chan net.Conn
	closed chan struct{}
	once   sync.Once
}

func newPipeListener() *pipeListener {
	return &pipeListener{conns: make(chan net.Conn), closed: make(chan struct{})}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.conns:
		return conn, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *pipeListener) Addr() net.Addr { return testAddr("pipe") }

type observedConn struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
}

func (c *observedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { close(c.closed) })
	return err
}

func connectPipe(t *testing.T, listener *pipeListener) (*wireClient, <-chan struct{}) {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() { client.Close() })
	if err := client.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	observed := &observedConn{Conn: server, closed: make(chan struct{})}
	select {
	case listener.conns <- observed:
	case <-time.After(time.Second):
		t.Fatal("server did not accept pipe")
	}
	return &wireClient{conn: client, reader: bufio.NewReader(client)}, observed.closed
}

func TestWriteDeadlineClosesClientThatDoesNotRead(t *testing.T) {
	store := newMemoryStore()
	store.values["key"] = bytes.Repeat([]byte{'x'}, engine.MaxValueSize)
	listener := newPipeListener()
	cfg := DefaultConfig()
	cfg.WriteTimeout = 30 * time.Millisecond
	startServerOnListener(t, store, &recordingLimiter{}, cfg, listener)
	client, closed := connectPipe(t, listener)
	if _, err := client.conn.Write(requestBytes(t, protocol.Request{Op: "GET", Key: []byte("key")})); err != nil {
		t.Fatal(err)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("blocked response did not honor write deadline")
	}
	if store.callCount() != 1 {
		t.Fatal("GET did not reach storage")
	}
}

func TestReadDeadlineBoundsSlowPartialFrame(t *testing.T) {
	store := newMemoryStore()
	listener := newPipeListener()
	cfg := DefaultConfig()
	cfg.ReadTimeout = 100 * time.Millisecond
	startServerOnListener(t, store, &recordingLimiter{}, cfg, listener)
	client, closed := connectPipe(t, listener)
	writing := make(chan struct{})
	go func() {
		defer close(writing)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-closed:
				return
			case <-ticker.C:
				if _, err := client.conn.Write([]byte{' '}); err != nil {
					return
				}
			}
		}
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("partial-frame progress extended read deadline")
	}
	<-writing
	if store.callCount() != 0 {
		t.Fatal("partial frame reached storage")
	}
}

func TestShutdownWaitsForResponseWrite(t *testing.T) {
	store := newMemoryStore()
	entered := make(chan struct{})
	store.hook = func(string, []byte) error { close(entered); return nil }
	listener := newPipeListener()
	running := startServerOnListener(t, store, &recordingLimiter{}, DefaultConfig(), listener)
	client, _ := connectPipe(t, listener)
	if _, err := client.conn.Write(requestBytes(t, protocol.Request{Op: "SET", Key: []byte("key")})); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("command was not admitted")
	}
	done := make(chan error, 1)
	go func() { done <- running.server.Shutdown(context.Background()) }()
	waitForClosing(t, running.server)
	select {
	case err := <-done:
		t.Fatalf("shutdown returned while response was blocked: %v", err)
	default:
	}
	requireOK(t, client.response(t))
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish after response was read")
	}
}

func TestShutdownBeforeServeAndRepeatedCalls(t *testing.T) {
	s, err := New(newMemoryStore(), &recordingLimiter{}, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := s.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	listener := newPipeListener()
	if err := s.Serve(listener); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("Serve = %v; want ErrServerClosed", err)
	}
	select {
	case <-listener.closed:
	default:
		t.Fatal("Serve after shutdown retained listener")
	}
}

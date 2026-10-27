package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Ahmad-Mosha/kv-store/internal/protocol"
)

func serveOnce(t *testing.T, handler func(net.Conn)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		handler(conn)
	}()
	t.Cleanup(func() {
		listener.Close()
		<-done
	})
	return listener.Addr().String()
}

func TestCLICommands(t *testing.T) {
	value := []byte("Cairo")
	exists := true
	for _, test := range []struct {
		name     string
		args     []string
		request  protocol.Request
		response protocol.Response
		output   string
	}{
		{"set", []string{"set", "city", "Cairo"}, protocol.Request{Op: "SET", Key: []byte("city"), Value: value}, protocol.Response{OK: true}, "OK\n"},
		{"empty set", []string{"set", "city", ""}, protocol.Request{Op: "SET", Key: []byte("city"), Value: []byte{}}, protocol.Response{OK: true}, "OK\n"},
		{"get", []string{"get", "city"}, protocol.Request{Op: "GET", Key: []byte("city")}, protocol.Response{OK: true, Value: &value}, "Cairo\n"},
		{"delete", []string{"delete", "city"}, protocol.Request{Op: "DELETE", Key: []byte("city")}, protocol.Response{OK: true}, "OK\n"},
		{"exists", []string{"exists", "city"}, protocol.Request{Op: "EXISTS", Key: []byte("city")}, protocol.Response{OK: true, Exists: &exists}, "true\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			addr := serveOnce(t, func(conn net.Conn) {
				request, err := protocol.NewReader(conn).ReadRequest()
				if err != nil {
					t.Error(err)
					return
				}
				if request.Op != test.request.Op || !bytes.Equal(request.Key, test.request.Key) || !bytes.Equal(request.Value, test.request.Value) {
					t.Errorf("request = %#v, want %#v", request, test.request)
				}
				if err := protocol.WriteResponse(conn, test.response); err != nil {
					t.Error(err)
				}
			})
			var stdout bytes.Buffer
			if err := run(context.Background(), append([]string{"-addr", addr}, test.args...), &stdout, io.Discard); err != nil {
				t.Fatal(err)
			}
			if stdout.String() != test.output {
				t.Fatalf("output = %q, want %q", stdout.String(), test.output)
			}
		})
	}
}

func TestCLIBase64BinaryAndEmptyValues(t *testing.T) {
	for _, value := range [][]byte{{0, 0xff, '\n'}, {}} {
		key := []byte{0xff, 0, 1}
		addr := serveOnce(t, func(conn net.Conn) {
			request, err := protocol.NewReader(conn).ReadRequest()
			if err != nil {
				t.Error(err)
				return
			}
			if !bytes.Equal(request.Key, key) {
				t.Errorf("key = %v, want %v", request.Key, key)
			}
			if err := protocol.WriteResponse(conn, protocol.Response{OK: true, Value: &value}); err != nil {
				t.Error(err)
			}
		})
		var stdout bytes.Buffer
		err := run(context.Background(), []string{"-addr", addr, "-base64", "get", base64.StdEncoding.EncodeToString(key)}, &stdout, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		if want := base64.StdEncoding.EncodeToString(value) + "\n"; stdout.String() != want {
			t.Fatalf("output = %q, want %q", stdout.String(), want)
		}
	}
}

func TestCLIRejectsInvalidInputBeforeConnecting(t *testing.T) {
	for _, args := range [][]string{
		{}, {"set", "key"}, {"get", "key", "extra"}, {"unknown", "key"}, {"get", ""},
		{"-timeout", "0", "get", "key"}, {"-timeout", "25h", "get", "key"}, {"-addr", "", "get", "key"},
		{"-addr", "localhost:invalid", "get", "key"}, {"-addr", "localhost:65536", "get", "key"},
		{"-base64", "get", "!!!"}, {"-base64", "set", "aw==", "!!!"},
		{"get", strings.Repeat("k", protocol.MaxKeyBytes+1)},
		{"set", "key", strings.Repeat("v", protocol.MaxValueBytes+1)},
	} {
		err := run(context.Background(), append([]string{"-addr", "127.0.0.1:1"}, args...), io.Discard, io.Discard)
		if err == nil || strings.Contains(err.Error(), "connect:") {
			t.Fatalf("args %q returned %v; expected validation before dialing", args, err)
		}
	}
	if err := run(context.Background(), []string{"-h"}, io.Discard, io.Discard); err != flag.ErrHelp {
		t.Fatalf("help = %v", err)
	}
}

func TestCLITimeoutIncludesHostnameResolution(t *testing.T) {
	original := net.DefaultResolver
	started := make(chan struct{}, 2)
	net.DefaultResolver = &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	defer func() { net.DefaultResolver = original }()
	if _, err := parseOptions([]string{"-addr", "diskkv-timeout.invalid:7379", "get", "key"}, io.Discard); err != nil {
		t.Fatalf("hostname syntax validation performed a lookup: %v", err)
	}
	begin := time.Now()
	err := run(context.Background(), []string{"-addr", "diskkv-timeout.invalid:7379", "-timeout", "50ms", "get", "key"}, io.Discard, io.Discard)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("hostname lookup error = %v", err)
	}
	select {
	case <-started:
	default:
		t.Fatal("hostname lookup did not use the test resolver")
	}
	if elapsed := time.Since(begin); elapsed > 5*time.Second {
		t.Fatalf("lookup exceeded request timeout: %v", elapsed)
	}
}

func TestCLIReportsRemoteErrorsAndRetry(t *testing.T) {
	addr := serveOnce(t, func(conn net.Conn) {
		if _, err := protocol.NewReader(conn).ReadRequest(); err != nil {
			t.Error(err)
			return
		}
		protocol.WriteResponse(conn, protocol.Response{Error: &protocol.Error{Code: "RATE_LIMITED", Message: "try later", RetryAfterMS: 123}})
	})
	err := run(context.Background(), []string{"-addr", addr, "get", "key"}, io.Discard, io.Discard)
	if err == nil || err.Error() != "RATE_LIMITED: try later (retry after 123 ms)" {
		t.Fatalf("error = %v", err)
	}
}

func TestCLIMutationStorageErrorsReportUncertainOutcome(t *testing.T) {
	for _, test := range []struct {
		op        string
		code      string
		uncertain bool
	}{
		{"set", protocol.CodeUnavailable, true},
		{"set", protocol.CodeInternal, true},
		{"delete", protocol.CodeUnavailable, true},
		{"delete", protocol.CodeInternal, true},
		{"set", protocol.CodeBadRequest, false},
		{"delete", protocol.CodeRateLimited, false},
		{"get", protocol.CodeUnavailable, false},
	} {
		t.Run(test.op+"/"+test.code, func(t *testing.T) {
			addr := serveOnce(t, func(conn net.Conn) {
				if _, err := protocol.NewReader(conn).ReadRequest(); err != nil {
					t.Error(err)
					return
				}
				if err := protocol.WriteResponse(conn, protocol.Response{Error: &protocol.Error{Code: test.code, Message: "failed"}}); err != nil {
					t.Error(err)
				}
			})
			args := []string{"-addr", addr, test.op, "key"}
			if test.op == "set" {
				args = append(args, "value")
			}
			err := run(context.Background(), args, io.Discard, io.Discard)
			if err == nil || strings.Contains(err.Error(), "mutation outcome may be uncertain") != test.uncertain {
				t.Fatalf("error = %v, want uncertain=%v", err, test.uncertain)
			}
		})
	}
}

func TestCLIRejectsMissingResponseFields(t *testing.T) {
	for _, command := range []string{"get", "exists"} {
		addr := serveOnce(t, func(conn net.Conn) {
			protocol.NewReader(conn).ReadRequest()
			protocol.WriteResponse(conn, protocol.Response{OK: true})
		})
		if err := run(context.Background(), []string{"-addr", addr, command, "key"}, io.Discard, io.Discard); err == nil {
			t.Fatalf("%s accepted missing result", command)
		}
	}
}

func TestCLILostMutationResponseIsUncertain(t *testing.T) {
	addr := serveOnce(t, func(conn net.Conn) {
		if _, err := protocol.NewReader(conn).ReadRequest(); err != nil {
			t.Error(err)
		}
	})
	err := run(context.Background(), []string{"-addr", addr, "set", "key", "value"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "mutation outcome may be uncertain") {
		t.Fatalf("error = %v", err)
	}
}

func TestCLITimeoutAndCancellationInterruptRead(t *testing.T) {
	for _, cancelEarly := range []bool{false, true} {
		t.Run(map[bool]string{false: "deadline", true: "cancellation"}[cancelEarly], func(t *testing.T) {
			started := make(chan struct{})
			release := make(chan struct{})
			defer close(release)
			addr := serveOnce(t, func(conn net.Conn) {
				protocol.NewReader(conn).ReadRequest()
				close(started)
				<-release
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			args := []string{"-addr", addr, "-timeout", "50ms", "get", "key"}
			if cancelEarly {
				args = []string{"-addr", addr, "-timeout", "5s", "get", "key"}
				go func() { <-started; cancel() }()
			}
			if err := run(ctx, args, io.Discard, io.Discard); err == nil {
				t.Fatal("expected interrupted read")
			}
		})
	}
}

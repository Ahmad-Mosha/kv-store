package main

import (
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Ahmad-Mosha/kv-store/internal/protocol"
)

type options struct {
	addr    string
	timeout time.Duration
	base64  bool
	request protocol.Request
}

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseOptions(args []string, stderr io.Writer) (options, error) {
	var opts options
	flags := flag.NewFlagSet("diskkv-cli", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&opts.addr, "addr", "127.0.0.1:7379", "TCP server address")
	flags.DurationVar(&opts.timeout, "timeout", 5*time.Second, "total connection/request timeout")
	flags.BoolVar(&opts.base64, "base64", false, "decode key/value arguments and encode GET output as base64")
	flags.Usage = func() {
		fmt.Fprintln(stderr, "Usage: diskkv-cli [flags] set key value | get key | delete key | exists key")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	if opts.timeout <= 0 || opts.timeout > 24*time.Hour {
		return options{}, errors.New("timeout must be positive and at most 24h")
	}
	if opts.addr == "" {
		return options{}, errors.New("server address must not be empty")
	}
	_, port, err := net.SplitHostPort(opts.addr)
	if err != nil {
		return options{}, fmt.Errorf("invalid server address: %w", err)
	}
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return options{}, fmt.Errorf("invalid server port: %w", err)
	}
	command := flags.Args()
	if len(command) == 0 {
		return options{}, errors.New("command required: set, get, delete, or exists")
	}
	opts.request.Op = strings.ToUpper(command[0])
	expected := 2
	if opts.request.Op == "SET" {
		expected = 3
	} else if opts.request.Op != "GET" && opts.request.Op != "DELETE" && opts.request.Op != "EXISTS" {
		return options{}, fmt.Errorf("unknown command %q", command[0])
	}
	if len(command) != expected {
		return options{}, fmt.Errorf("%s requires %d argument(s)", strings.ToLower(opts.request.Op), expected-1)
	}
	decode := func(text string) ([]byte, error) {
		if opts.base64 {
			return base64.StdEncoding.DecodeString(text)
		}
		return []byte(text), nil
	}
	opts.request.Key, err = decode(command[1])
	if err != nil {
		return options{}, fmt.Errorf("decode key: %w", err)
	}
	if opts.request.Op == "SET" {
		opts.request.Value, err = decode(command[2])
		if err != nil {
			return options{}, fmt.Errorf("decode value: %w", err)
		}
	}
	if err := protocol.ValidateRequest(opts.request); err != nil {
		return options{}, err
	}
	return opts, nil
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	opts, err := parseOptions(args, stderr)
	if err != nil {
		return err
	}
	requestCtx, cancel := context.WithTimeout(ctx, opts.timeout)
	defer cancel()
	var dialer net.Dialer
	conn, err := dialer.DialContext(requestCtx, "tcp", opts.addr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close()
	deadline, _ := requestCtx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return err
	}
	stop := context.AfterFunc(requestCtx, func() { conn.Close() })
	defer stop()
	if err := protocol.WriteRequest(conn, opts.request); err != nil {
		return requestError(opts.request.Op, "write request", err)
	}
	response, err := protocol.NewReader(conn).ReadResponse()
	if err != nil {
		return requestError(opts.request.Op, "read response", err)
	}
	if !response.OK {
		if response.Error == nil {
			return errors.New("invalid error response")
		}
		message := fmt.Sprintf("%s: %s", response.Error.Code, response.Error.Message)
		if response.Error.RetryAfterMS > 0 {
			message += fmt.Sprintf(" (retry after %d ms)", response.Error.RetryAfterMS)
		}
		if (opts.request.Op == "SET" || opts.request.Op == "DELETE") &&
			(response.Error.Code == protocol.CodeUnavailable || response.Error.Code == protocol.CodeInternal) {
			message += "; mutation outcome may be uncertain"
		}
		return errors.New(message)
	}
	switch opts.request.Op {
	case "GET":
		if response.Value == nil {
			return errors.New("GET response is missing a value")
		}
		value := *response.Value
		if opts.base64 {
			_, err = fmt.Fprintln(stdout, base64.StdEncoding.EncodeToString(value))
		} else {
			_, err = stdout.Write(append(value, '\n'))
		}
	case "EXISTS":
		if response.Exists == nil {
			return errors.New("EXISTS response is missing its result")
		}
		_, err = fmt.Fprintln(stdout, *response.Exists)
	default:
		_, err = fmt.Fprintln(stdout, "OK")
	}
	return err
}

func requestError(op, stage string, err error) error {
	if op == "SET" || op == "DELETE" {
		return fmt.Errorf("%s: mutation outcome may be uncertain: %w", stage, err)
	}
	return fmt.Errorf("%s: %w", stage, err)
}

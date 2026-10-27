package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Ahmad-Mosha/kv-store/engine"
	"github.com/Ahmad-Mosha/kv-store/internal/server"
)

type options struct {
	addr            string
	data            string
	burst           int
	rate            float64
	shutdownTimeout time.Duration
	server          server.Config
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stderr); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseOptions(args []string, stderr io.Writer) (options, error) {
	opts := options{server: server.DefaultConfig()}
	flags := flag.NewFlagSet("diskkv", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&opts.addr, "addr", "127.0.0.1:7379", "TCP listen address")
	flags.StringVar(&opts.data, "data", "./data", "database directory")
	flags.IntVar(&opts.burst, "burst", 100, "per-IP token bucket capacity")
	flags.Float64Var(&opts.rate, "rate", 50, "per-IP tokens refilled per second")
	flags.IntVar(&opts.server.MaxConnections, "max-connections", opts.server.MaxConnections, "maximum simultaneous connections")
	flags.DurationVar(&opts.server.ReadTimeout, "read-timeout", opts.server.ReadTimeout, "maximum time to read a request")
	flags.DurationVar(&opts.server.WriteTimeout, "write-timeout", opts.server.WriteTimeout, "maximum time to write a response")
	flags.DurationVar(&opts.shutdownTimeout, "shutdown-timeout", 5*time.Second, "maximum time to drain active requests")
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	if flags.NArg() != 0 {
		return options{}, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if opts.data == "" {
		return options{}, errors.New("data directory must not be empty")
	}
	if opts.addr == "" {
		return options{}, errors.New("listen address must not be empty")
	}
	_, port, err := net.SplitHostPort(opts.addr)
	if err != nil {
		return options{}, fmt.Errorf("invalid listen address: %w", err)
	}
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return options{}, fmt.Errorf("invalid listen port: %w", err)
	}
	if opts.shutdownTimeout <= 0 || opts.shutdownTimeout > 24*time.Hour {
		return options{}, errors.New("shutdown timeout must be positive and at most 24h")
	}
	if err := server.ValidateConfig(opts.server); err != nil {
		return options{}, err
	}
	if err := server.ValidateBucketConfig(opts.burst, opts.rate); err != nil {
		return options{}, err
	}
	return opts, nil
}

func run(ctx context.Context, args []string, stderr io.Writer) (err error) {
	opts, err := parseOptions(args, stderr)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(opts.data, 0700); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}
	db, err := engine.Open(opts.data)
	if err != nil {
		return err
	}
	closeDB := true
	defer func() {
		if closeDB {
			err = errors.Join(err, db.Close())
		}
	}()
	bucket, err := server.NewBucket(opts.burst, opts.rate)
	if err != nil {
		return err
	}
	defer bucket.Stop()
	srv, err := server.New(db, bucket, opts.server)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", opts.addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	fmt.Fprintf(stderr, "listening on %s\n", listener.Addr())
	closeDB, err = serve(ctx, srv, listener, opts.shutdownTimeout)
	return err
}

func serve(ctx context.Context, srv *server.Server, listener net.Listener, timeout time.Duration) (bool, error) {
	done := make(chan error, 1)
	go func() { done <- srv.Serve(listener) }()
	var serveErr error
	exited := false
	select {
	case <-ctx.Done():
	case serveErr = <-done:
		exited = true
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		// Active file I/O cannot be canceled; waiting in DB.Close would defeat the deadline.
		return false, errors.Join(normalizeServeError(serveErr), fmt.Errorf("shutdown incomplete; active operation outcomes may be uncertain: %w", err))
	}
	if !exited {
		serveErr = <-done
	}
	return true, normalizeServeError(serveErr)
}

func normalizeServeError(err error) error {
	if errors.Is(err, server.ErrServerClosed) {
		return nil
	}
	return err
}

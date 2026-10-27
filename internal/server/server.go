package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/Ahmad-Mosha/kv-store/internal/protocol"
	"github.com/Ahmad-Mosha/rate-limiter/limiter"
)

var (
	ErrServerClosed   = errors.New("diskkv: server closed")
	ErrAlreadyServing = errors.New("diskkv: server already started")
)

type Store interface {
	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Exists(key []byte) (bool, error)
}

type connection struct {
	active bool
}

type Server struct {
	store   Store
	limiter limiter.RateLimiter
	config  Config
	ctx     context.Context
	cancel  context.CancelFunc

	mu        sync.Mutex
	listener  net.Listener
	conns     map[net.Conn]*connection
	started   bool
	accepting bool
	closing   bool
	finished  bool
	done      chan struct{}
}

func New(store Store, rateLimiter limiter.RateLimiter, config Config) (*Server, error) {
	if err := ValidateConfig(config); err != nil {
		return nil, err
	}
	if store == nil || rateLimiter == nil {
		return nil, errors.New("store and rate limiter are required")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		store: store, limiter: rateLimiter, config: config, ctx: ctx, cancel: cancel,
		conns: make(map[net.Conn]*connection), done: make(chan struct{}),
	}, nil
}

// Serve takes ownership of the listener once started. A server cannot be restarted.
func (s *Server) Serve(listener net.Listener) error {
	if listener == nil {
		return errors.New("listener is required")
	}
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		listener.Close()
		return ErrServerClosed
	}
	if s.started {
		s.mu.Unlock()
		return ErrAlreadyServing
	}
	s.started, s.accepting, s.listener = true, true, listener
	s.mu.Unlock()
	defer func() {
		listener.Close()
		s.mu.Lock()
		s.accepting = false
		s.finishLocked()
		s.mu.Unlock()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			s.mu.Lock()
			closing := s.closing
			s.mu.Unlock()
			if closing {
				return ErrServerClosed
			}
			return fmt.Errorf("accept connection: %w", err)
		}
		s.mu.Lock()
		if s.closing || len(s.conns) >= s.config.MaxConnections {
			s.mu.Unlock()
			conn.Close()
			continue
		}
		s.conns[conn] = &connection{}
		s.mu.Unlock()
		go s.serveConnection(conn)
	}
}

func (s *Server) serveConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.finishLocked()
		s.mu.Unlock()
	}()
	reader := protocol.NewReader(conn)
	key := peerKey(conn.RemoteAddr())
	for {
		if err := conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout)); err != nil {
			return
		}
		request, err := reader.ReadRequest()
		if err != nil {
			if errors.Is(err, protocol.ErrFrameTooLarge) {
				s.writeResponse(conn, failure(protocol.CodeFrameTooLarge, "frame exceeds maximum size"))
			} else if errors.Is(err, protocol.ErrInvalidFrame) {
				s.writeResponse(conn, failure(protocol.CodeBadRequest, "invalid request frame"))
			}
			return
		}
		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			return
		}
		s.conns[conn].active = true
		s.mu.Unlock()

		response := s.execute(request, key)
		if err := s.writeResponse(conn, response); err != nil {
			return
		}
		s.mu.Lock()
		s.conns[conn].active = false
		closing := s.closing
		s.mu.Unlock()
		if closing {
			return
		}
	}
}

func (s *Server) writeResponse(conn net.Conn, response protocol.Response) error {
	if err := conn.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout)); err != nil {
		return err
	}
	return protocol.WriteResponse(conn, response)
}

// Shutdown drains admitted commands; a deadline closes connections but cannot cancel filesystem I/O.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closing = true
	listener := s.listener
	var idle []net.Conn
	for conn, state := range s.conns {
		if !state.active {
			idle = append(idle, conn)
		}
	}
	s.finishLocked()
	s.mu.Unlock()
	if listener != nil {
		listener.Close()
	}
	for _, conn := range idle {
		conn.Close()
	}
	select {
	case <-s.done:
		return nil
	default:
	}
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		s.cancel()
		s.mu.Lock()
		conns := make([]net.Conn, 0, len(s.conns))
		for conn := range s.conns {
			conns = append(conns, conn)
		}
		s.mu.Unlock()
		for _, conn := range conns {
			conn.Close()
		}
		return ctx.Err()
	}
}

func (s *Server) finishLocked() {
	if s.closing && !s.accepting && len(s.conns) == 0 && !s.finished {
		s.finished = true
		s.cancel()
		close(s.done)
	}
}

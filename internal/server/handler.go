package server

import (
	"errors"
	"net"
	"net/netip"
	"time"

	"github.com/Ahmad-Mosha/kv-store/engine"
	"github.com/Ahmad-Mosha/kv-store/internal/protocol"
)

func peerKey(addr net.Addr) string {
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		return ip.Unmap().String()
	}
	return host
}

func failure(code, message string) protocol.Response {
	return protocol.Response{Error: &protocol.Error{Code: code, Message: message}}
}

func (s *Server) execute(request protocol.Request, key string) protocol.Response {
	result, err := s.limiter.Allow(s.ctx, key)
	if err != nil {
		return failure(protocol.CodeUnavailable, "rate limiter unavailable")
	}
	if !result.Allowed {
		response := failure(protocol.CodeRateLimited, "request limit exceeded")
		retryMS := result.RetryAfter.Milliseconds()
		if result.RetryAfter%time.Millisecond != 0 {
			retryMS++
		}
		response.Error.RetryAfterMS = max(retryMS, 1)
		return response
	}
	if err := protocol.ValidateRequest(request); err != nil {
		return failure(protocol.CodeBadRequest, "invalid command or key/value size")
	}
	response := protocol.Response{OK: true}
	switch request.Op {
	case "SET":
		err = s.store.Set(request.Key, request.Value)
	case "GET":
		var value []byte
		value, err = s.store.Get(request.Key)
		if value == nil {
			value = []byte{}
		}
		response.Value = &value
	case "DELETE":
		err = s.store.Delete(request.Key)
	case "EXISTS":
		var exists bool
		exists, err = s.store.Exists(request.Key)
		response.Exists = &exists
	}
	if err == nil {
		return response
	}
	switch {
	case errors.Is(err, engine.ErrNotFound):
		return failure(protocol.CodeNotFound, "key not found")
	case errors.Is(err, engine.ErrInvalidKey), errors.Is(err, engine.ErrValueTooLarge):
		return failure(protocol.CodeBadRequest, "invalid key/value size")
	case errors.Is(err, engine.ErrClosed), errors.Is(err, engine.ErrFaulted):
		return failure(protocol.CodeUnavailable, "storage unavailable")
	default:
		return failure(protocol.CodeInternal, "storage operation failed")
	}
}

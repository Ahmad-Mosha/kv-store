package server

import (
	"errors"
	"math"
	"time"

	"github.com/Ahmad-Mosha/rate-limiter/limiter"
)

type Config struct {
	MaxConnections int
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
}

func DefaultConfig() Config {
	return Config{MaxConnections: 128, ReadTimeout: 30 * time.Second, WriteTimeout: 5 * time.Second}
}

func ValidateConfig(config Config) error {
	if config.MaxConnections < 1 || config.MaxConnections > 4096 {
		return errors.New("max connections must be between 1 and 4096")
	}
	if config.ReadTimeout <= 0 || config.ReadTimeout > 24*time.Hour {
		return errors.New("read timeout must be positive and at most 24h")
	}
	if config.WriteTimeout <= 0 || config.WriteTimeout > 24*time.Hour {
		return errors.New("write timeout must be positive and at most 24h")
	}
	return nil
}

func ValidateBucketConfig(burst int, rate float64) error {
	if burst < 1 || burst > 1_000_000 {
		return errors.New("burst must be between 1 and 1000000")
	}
	if math.IsNaN(rate) || math.IsInf(rate, 0) || rate <= 0 {
		return errors.New("rate must be finite and positive")
	}
	// The upstream cleanup interval converts burst/rate to time.Duration.
	if float64(burst)/rate > (24 * time.Hour).Seconds() {
		return errors.New("burst/rate must refill within 24h")
	}
	return nil
}

func NewBucket(burst int, rate float64) (*limiter.Bucket, error) {
	if err := ValidateBucketConfig(burst, rate); err != nil {
		return nil, err
	}
	return limiter.NewBucket(float64(burst), rate), nil
}

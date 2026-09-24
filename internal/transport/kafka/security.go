package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// SecurityConfig is intentionally shared by Kafka producers and consumers.
// Leaving it empty preserves the existing plaintext, unauthenticated local
// development behaviour. TLS is always verified: this package never exposes
// an insecure-skip-verify option.
type SecurityConfig struct {
	TLS  TLSConfig
	SASL SASLConfig
}

// TLSConfig configures verified TLS. Exactly one of CAPEM and CAFile may be
// set. ClientCertFile and ClientKeyFile are an all-or-nothing pair.
type TLSConfig struct {
	Enabled        bool
	CAPEM          string
	CAFile         string
	ServerName     string
	ClientCertFile string
	ClientKeyFile  string
}

// SASLConfig supports the Kafka mechanisms appropriate for static username /
// password credentials. Credentials are never returned in validation errors.
type SASLConfig struct {
	Mechanism string
	Username  string
	Password  string
}

const (
	SASLPlain       = "PLAIN"
	SASLSCRAMSHA256 = "SCRAM-SHA-256"
	SASLSCRAMSHA512 = "SCRAM-SHA-512"
)

// clientSecurityOptions is deliberately separate from kgo.Opt so tests can
// prove the producer and consumer receive identical security material.
type clientSecurityOptions struct {
	TLS  *tls.Config
	SASL sasl.Mechanism
}

func (c SecurityConfig) clientOptions() (clientSecurityOptions, error) {
	tlsConfig, err := c.TLS.config()
	if err != nil {
		return clientSecurityOptions{}, err
	}
	mechanism, err := c.SASL.mechanism()
	if err != nil {
		return clientSecurityOptions{}, err
	}
	return clientSecurityOptions{TLS: tlsConfig, SASL: mechanism}, nil
}

func (o clientSecurityOptions) kgoOptions() []kgo.Opt {
	opts := make([]kgo.Opt, 0, 2)
	if o.TLS != nil {
		opts = append(opts, kgo.DialTLSConfig(o.TLS))
	}
	if o.SASL != nil {
		opts = append(opts, kgo.SASL(o.SASL))
	}
	return opts
}

func (c TLSConfig) config() (*tls.Config, error) {
	if !c.Enabled {
		if c.CAPEM != "" || c.CAFile != "" || c.ServerName != "" || c.ClientCertFile != "" || c.ClientKeyFile != "" {
			return nil, errors.New("Kafka TLS settings require TLS to be enabled")
		}
		return nil, nil
	}
	if c.CAPEM != "" && c.CAFile != "" {
		return nil, errors.New("Kafka TLS CA PEM and CA file cannot both be set")
	}
	if (c.ClientCertFile == "") != (c.ClientKeyFile == "") {
		return nil, errors.New("Kafka TLS client certificate and key must be set together")
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: strings.TrimSpace(c.ServerName)}
	if c.CAPEM != "" || c.CAFile != "" {
		pem := []byte(c.CAPEM)
		if c.CAFile != "" {
			var err error
			pem, err = os.ReadFile(c.CAFile)
			if err != nil {
				return nil, fmt.Errorf("read Kafka TLS CA file: %w", err)
			}
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("parse Kafka TLS CA certificate")
		}
		config.RootCAs = pool
	}
	if c.ClientCertFile != "" {
		cert, err := tls.LoadX509KeyPair(c.ClientCertFile, c.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load Kafka TLS client certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{cert}
	}
	return config, nil
}

func (c SASLConfig) mechanism() (sasl.Mechanism, error) {
	mechanism := strings.ToUpper(strings.TrimSpace(c.Mechanism))
	if mechanism == "" {
		if c.Username != "" || c.Password != "" {
			return nil, errors.New("Kafka SASL username and password require a mechanism")
		}
		return nil, nil
	}
	if c.Username == "" || c.Password == "" {
		return nil, errors.New("Kafka SASL username and password are required")
	}
	switch mechanism {
	case SASLPlain:
		return plain.Auth{User: c.Username, Pass: c.Password}.AsMechanism(), nil
	case SASLSCRAMSHA256:
		return scram.Auth{User: c.Username, Pass: c.Password}.AsSha256Mechanism(), nil
	case SASLSCRAMSHA512:
		return scram.Auth{User: c.Username, Pass: c.Password}.AsSha512Mechanism(), nil
	default:
		return nil, errors.New("unsupported Kafka SASL mechanism")
	}
}

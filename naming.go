// Package grpcnacos provides gRPC service registration and discovery on top
// of Nacos: a Registrar that publishes instances and a resolver.Builder that
// turns a service name into the addresses of its healthy instances.
package grpcnacos

import (
	"context"
)

// Registrar registers a service instance and removes it again.
type Registrar interface {
	// Register publishes the service instance.
	Register(ctx context.Context) error
	// Deregister removes the service instance.
	Deregister(ctx context.Context) error
}

// Factory builds a Registrar from a DSN.
type Factory interface {
	// New parses dsn and returns a Registrar for it.
	New(ctx context.Context, dsn string) (Registrar, error)
}

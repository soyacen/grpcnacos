// Command server registers a gRPC health server in Nacos and serves it.
//
// The instance is registered under the address it listens on, and the health
// server only reports SERVING for that address: a check for the address of
// another instance of the service answers NOT_FOUND. example/client relies on
// that to prove that the Nacos resolver published every instance.
//
// The registrar DSN is read from GRPCNACOS_DSN and defaults to
//
//	nacos://127.0.0.1:8848/grpcnacos-example?group=DEFAULT_GROUP&ip=127.0.0.1&port=9101
//
// A second server needs a different DSN, for example with port=9102, because
// the port of the DSN is the port the server listens on. The instance is
// deregistered again on SIGINT and SIGTERM.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/soyacen/grpcnacos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// defaultDsn registers grpcnacos-example in a local Nacos and serves on
// 127.0.0.1:9101.
const defaultDsn = "nacos://127.0.0.1:8848/grpcnacos-example?group=DEFAULT_GROUP&ip=127.0.0.1&port=9101"

func main() {
	if err := run(); err != nil {
		log.Fatalf("example/server: %v", err)
	}
}

func run() error {
	dsn := os.Getenv("GRPCNACOS_DSN")
	if dsn == "" {
		dsn = defaultDsn
	}

	// The signal context only triggers the shutdown below; everything that has
	// to happen before it reports its own failure through run.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	parsed, err := grpcnacos.ParseDsn(ctx, "registrar", dsn)
	if err != nil {
		return err
	}

	addr := net.JoinHostPort(parsed.RegisterParam.Ip, strconv.FormatUint(parsed.RegisterParam.Port, 10))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	// The id of the health service is the address the instance is reachable
	// on, so that a client can tell which instance answered a check.
	healthServer := health.NewServer()
	healthServer.SetServingStatus(addr, healthpb.HealthCheckResponse_SERVING)

	server := grpc.NewServer()
	healthpb.RegisterHealthServer(server, healthServer)
	go func() {
		// Serve returns ErrServerStopped once GracefulStop below runs.
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Printf("serve: %v", err)
		}
	}()

	log.Printf("registering %s as %s of group %s", addr, parsed.RegisterParam.ServiceName, parsed.RegisterParam.GroupName)

	registrar, err := grpcnacos.NewRegistrar(dsn)
	if err != nil {
		return err
	}
	if err := registrar.Register(ctx); err != nil {
		return err
	}
	fmt.Printf("listening %s\n", addr)

	<-ctx.Done()
	// A second signal should kill the process instead of being swallowed while
	// the instance is being deregistered.
	stop()

	if err := registrar.Deregister(context.Background()); err != nil {
		log.Printf("deregister: %v", err)
	}
	server.GracefulStop()
	fmt.Println("stopping")

	return nil
}

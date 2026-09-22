// Command client resolves a Nacos service through grpcnacos and round-robins
// gRPC health checks over its instances.
//
// GRPCNACOS_DSN is both the DSN of the service to resolve and the gRPC target
// to dial, and defaults to
//
//	nacos://127.0.0.1:8848/grpcnacos-example?group=DEFAULT_GROUP
//
// GRPCNACOS_INSTANCES lists the instance ids to probe: "host:port" entries
// separated by commas, defaulting to 127.0.0.1:9101,127.0.0.1:9102. Every id is
// checked 20 times per round. example/server answers SERVING for its own id and
// NOT_FOUND for the id of another instance, so an id that reports both was
// reached through the instance owning it and through another one, which is only
// possible if the resolver published every instance of the service.
//
// The process exits non-zero only when the gRPC client cannot be created; the
// "round-robin ok" / "round-robin pending" line it prints is what the caller
// judges.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/soyacen/grpcnacos" // registers the "nacos" gRPC resolver
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

const (
	// defaultDsn resolves the service example/server registers by default.
	defaultDsn = "nacos://127.0.0.1:8848/grpcnacos-example?group=DEFAULT_GROUP"
	// defaultInstances are the ids of two example/server instances started
	// with their default DSNs, on 127.0.0.1:9101 and 127.0.0.1:9102.
	defaultInstances = "127.0.0.1:9101,127.0.0.1:9102"

	// checkTimeout bounds a single health check.
	checkTimeout = 5 * time.Second
	// warmUpRounds and warmUpDelay bound the wait for the instances to answer.
	warmUpRounds = 10
	warmUpDelay  = 500 * time.Millisecond
	// checksPerInstance is how often every instance id is checked per round.
	checksPerInstance = 20
	// maxRounds is how often the whole measurement is repeated when a round
	// does not show both instances being used.
	maxRounds = 3
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("example/client: %v", err)
	}
}

func run() error {
	dsn := os.Getenv("GRPCNACOS_DSN")
	if dsn == "" {
		dsn = defaultDsn
	}
	instances := os.Getenv("GRPCNACOS_INSTANCES")
	if instances == "" {
		instances = defaultInstances
	}
	ids := instanceIDs(instances)

	conn, err := grpc.NewClient(dsn,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`))
	if err != nil {
		return err
	}
	defer func() {
		_ = conn.Close()
	}()

	fmt.Printf("target %s\n", dsn)
	client := healthpb.NewHealthClient(conn)
	warmUp(context.Background(), client, ids)

	ok := false
	for round := 1; round <= maxRounds && !ok; round++ {
		if round > 1 {
			fmt.Printf("retrying (round %d)\n", round)
		}
		results := measure(context.Background(), client, ids)
		for _, r := range results {
			fmt.Printf("instance %s serving=%d notfound=%d other=%d\n", r.id, r.serving, r.notfound, r.other)
		}
		ok = allUsed(results)
	}

	if ok {
		fmt.Println("round-robin ok")
	} else {
		fmt.Println("round-robin pending")
	}

	return nil
}

// instanceIDs splits the comma separated instance id list, dropping empty
// entries.
func instanceIDs(raw string) []string {
	var ids []string
	for _, id := range strings.Split(raw, ",") {
		if id = strings.TrimSpace(id); id != "" {
			ids = append(ids, id)
		}
	}

	return ids
}

// outcome is what a single health check told us about the instance behind it.
type outcome int

const (
	// outcomeServing: the instance reached serves the id it was asked about.
	outcomeServing outcome = iota
	// outcomeNotFound: the instance reached does not serve that id, so it is
	// another instance of the service.
	outcomeNotFound
	// outcomeOther: no usable answer, for example because no subconnection was
	// ready yet or the resolver had no address to pick.
	outcomeOther
)

// check asks the instance picked by round-robin for the health of id.
func check(ctx context.Context, client healthpb.HealthClient, id string) outcome {
	ctx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()

	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: id})
	switch {
	case err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING:
		return outcomeServing
	case status.Code(err) == codes.NotFound:
		return outcomeNotFound
	default:
		return outcomeOther
	}
}

// warmUp blocks until every instance id has been served at least once, or until
// the attempts run out. An id that is served proves that the subconnection of
// the instance owning it is ready and that the resolver published that
// instance: that has to be the case before picks are counted. An id that is only
// ever answered NOT_FOUND belongs to an instance that is not published (any
// more); it is reported once and then measured like every other id.
func warmUp(ctx context.Context, client healthpb.HealthClient, ids []string) {
	served := make(map[string]bool, len(ids))
	for range warmUpRounds {
		for _, id := range ids {
			if !served[id] && check(ctx, client, id) == outcomeServing {
				served[id] = true
			}
		}
		if len(served) == len(ids) {
			return
		}
		time.Sleep(warmUpDelay)
	}

	var missing []string
	for _, id := range ids {
		if !served[id] {
			missing = append(missing, id)
		}
	}
	fmt.Printf("warm-up: no instance served %s\n", strings.Join(missing, ", "))
}

// instanceResult counts the outcomes of the checks issued for one instance id.
type instanceResult struct {
	id       string
	serving  int
	notfound int
	other    int
}

// measure checks every id checksPerInstance times and counts the outcomes.
func measure(ctx context.Context, client healthpb.HealthClient, ids []string) []instanceResult {
	results := make([]instanceResult, 0, len(ids))
	for _, id := range ids {
		r := instanceResult{id: id}
		for range checksPerInstance {
			switch check(ctx, client, id) {
			case outcomeServing:
				r.serving++
			case outcomeNotFound:
				r.notfound++
			default:
				r.other++
			}
		}
		results = append(results, r)
	}

	return results
}

// allUsed reports whether the measurement shows that every id was reached
// through both the instance serving it and another instance: the first proves
// that instance is in the resolver's address list, the second that it is not
// the only one.
func allUsed(results []instanceResult) bool {
	if len(results) == 0 {
		return false
	}
	for _, r := range results {
		if r.serving == 0 || r.notfound == 0 {
			return false
		}
	}

	return true
}

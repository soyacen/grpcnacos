package grpcnacos

import (
	"context"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	grpcresolver "google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
)

// The environment variables that enable the integration tests and the Nacos
// group they all register in.
const (
	testAddrEnv     = "GRPCNACOS_TEST_ADDR"
	testUsernameEnv = "GRPCNACOS_TEST_USERNAME"
	testPasswordEnv = "GRPCNACOS_TEST_PASSWORD"
	testGroup       = "GRPCNACOS"

	// pushTimeout is how long a registration or a resolution change may take to
	// reach a client.
	pushTimeout = 30 * time.Second
)

// testNacos describes the Nacos instance used by the integration tests.
type testNacos struct {
	host     string
	port     uint64
	username string
	password string
}

// newTestNacos skips the test unless GRPCNACOS_TEST_ADDR points to a server.
func newTestNacos(t *testing.T) testNacos {
	t.Helper()

	addr := os.Getenv(testAddrEnv)
	if addr == "" {
		t.Skipf("%s is not set, skipping the Nacos integration test", testAddrEnv)
	}

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("invalid %s %q: %v", testAddrEnv, addr, err)
	}
	value, err := strconv.ParseUint(port, 10, 64)
	if err != nil {
		t.Fatalf("invalid port in %s %q: %v", testAddrEnv, addr, err)
	}

	return testNacos{
		host:     host,
		port:     value,
		username: os.Getenv(testUsernameEnv),
		password: os.Getenv(testPasswordEnv),
	}
}

// endpoint returns the nacos:// endpoint of the test server, with credentials
// when the environment carries them.
func (n testNacos) endpoint() url.URL {
	endpoint := url.URL{
		Scheme: scheme,
		Host:   net.JoinHostPort(n.host, strconv.FormatUint(n.port, 10)),
	}
	if n.username != "" {
		endpoint.User = url.UserPassword(n.username, n.password)
	}

	return endpoint
}

// baseQuery returns the query parameters shared by the registrar and resolver
// DSNs. The SDK directories live in sdkDir: a client built by NewRegistrar has
// no close, so its log writer would race with t.TempDir cleanup.
func (n testNacos) baseQuery(t *testing.T) url.Values {
	t.Helper()

	query := url.Values{}
	query.Set("group", testGroup)
	query.Set("logDir", filepath.Join(sdkDir, t.Name(), "log"))
	query.Set("cacheDir", filepath.Join(sdkDir, t.Name(), "cache"))
	query.Set("logLevel", "error")
	query.Set("notLoadCacheAtStart", "true")

	return query
}

// registrarDsn builds the DSN that registers ip:port as service on the test
// server.
func (n testNacos) registrarDsn(t *testing.T, service, ip string, port uint64) string {
	t.Helper()

	endpoint := n.endpoint()
	endpoint.Path = "/" + service

	query := n.baseQuery(t)
	query.Set("ip", ip)
	query.Set("port", strconv.FormatUint(port, 10))
	endpoint.RawQuery = query.Encode()

	return endpoint.String()
}

// resolverDsn builds the DSN that resolves service from the test server.
func (n testNacos) resolverDsn(t *testing.T, service string) string {
	t.Helper()

	endpoint := n.endpoint()
	endpoint.Path = "/" + service
	endpoint.RawQuery = n.baseQuery(t).Encode()

	return endpoint.String()
}

// observer returns a Nacos client that is not built by the code under test, so
// the tests can verify registrations without asking the library about itself.
func (n testNacos) observer(t *testing.T) naming_client.INamingClient {
	t.Helper()

	options := []constant.ClientOption{
		constant.WithTimeoutMs(uint64((10 * time.Second).Milliseconds())),
		constant.WithNamespaceId(defaultNamespace),
		constant.WithLogDir(filepath.Join(sdkDir, t.Name(), "log")),
		constant.WithCacheDir(filepath.Join(sdkDir, t.Name(), "cache")),
		constant.WithLogLevel("error"),
		constant.WithNotLoadCacheAtStart(true),
		// Without this the client drops pushes carrying an empty host list, so
		// a deregistered instance would stay visible to the observer forever.
		constant.WithUpdateCacheWhenEmpty(true),
	}
	if n.username != "" {
		options = append(options, constant.WithUsername(n.username), constant.WithPassword(n.password))
	}

	client, err := clients.NewNamingClient(vo.NacosClientParam{
		ClientConfig:  constant.NewClientConfig(options...),
		ServerConfigs: []constant.ServerConfig{*constant.NewServerConfig(n.host, n.port)},
	})
	if err != nil {
		t.Fatalf("create observer naming client: %v", err)
	}
	t.Cleanup(client.CloseClient)

	return client
}

// waitForInstances polls the healthy instances of service until there are want
// of them, and fails the test when that does not happen within pushTimeout.
func (n testNacos) waitForInstances(t *testing.T, client naming_client.INamingClient, service string, want int) []model.Instance {
	t.Helper()

	deadline := time.Now().Add(pushTimeout)
	var lastErr error

	for {
		instances, err := client.SelectInstances(vo.SelectInstancesParam{
			ServiceName: service,
			GroupName:   testGroup,
			HealthyOnly: true,
		})
		switch {
		case err != nil:
			// Nacos reports an empty instance list as the error
			// "instance list is empty!", so a failed select with want == 0 is
			// the empty service the test is waiting for.
			if want == 0 {
				return nil
			}
			lastErr = err
		case len(instances) == want:
			return instances
		}

		if time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("service %s has no state with %d instances (last error: %v) after %s", service, want, lastErr, pushTimeout)

	return nil
}

// integrationConn records the states a resolver publishes, and lets a test wait
// for a given number of addresses.
type integrationConn struct {
	grpcresolver.ClientConn // the resolver only ever calls UpdateState

	mu     sync.Mutex
	states []grpcresolver.State
}

// newIntegrationConn returns a ClientConn that starts without any state.
func newIntegrationConn() *integrationConn {
	return &integrationConn{}
}

// UpdateState records a state published by the resolver.
func (c *integrationConn) UpdateState(state grpcresolver.State) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.states = append(c.states, state)

	return nil
}

// ReportError satisfies resolver.ClientConn; the Nacos resolver never reports
// errors through it.
func (c *integrationConn) ReportError(error) {}

// last returns the most recently published state, or the zero state when the
// resolver has not published anything yet.
func (c *integrationConn) last() grpcresolver.State {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.states) == 0 {
		return grpcresolver.State{}
	}

	return c.states[len(c.states)-1]
}

// waitForUpdate waits for the first state published by the resolver.
func (c *integrationConn) waitForUpdate(t *testing.T, timeout time.Duration) grpcresolver.State {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		c.mu.Lock()
		published := len(c.states) > 0
		c.mu.Unlock()

		if published {
			return c.last()
		}
		if time.Now().After(deadline) {
			t.Fatalf("resolver published no state within %s", timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// waitForAddresses waits until the most recent state published by the resolver
// holds want addresses, and returns it.
func (c *integrationConn) waitForAddresses(t *testing.T, want int, timeout time.Duration) grpcresolver.State {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		state := c.last()
		if len(state.Addresses) == want {
			return state
		}
		if time.Now().After(deadline) {
			t.Fatalf("resolver published %v, want %d addresses, after %s", addressList(state), want, timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// addressList flattens the addresses of a resolver state.
func addressList(state grpcresolver.State) []string {
	addrs := make([]string, 0, len(state.Addresses))
	for _, addr := range state.Addresses {
		addrs = append(addrs, addr.Addr)
	}

	return addrs
}

// startTestServer starts a local gRPC server whose health service reports
// serving for id, and returns the address it listens on together with a stop
// function. An empty id makes the server report its own listening address,
// which is what lets the tests tell two instances apart: a health server
// answers SERVING only for the addresses it was told about.
func startTestServer(t *testing.T, id string) (addr string, stop func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on a local port: %v", err)
	}
	if id == "" {
		id = ln.Addr().String()
	}

	hs := health.NewServer()
	hs.SetServingStatus(id, healthpb.HealthCheckResponse_SERVING)

	srv := grpc.NewServer()
	healthpb.RegisterHealthServer(srv, hs)
	go func() {
		_ = srv.Serve(ln)
	}()

	var once sync.Once

	return ln.Addr().String(), func() {
		once.Do(func() {
			srv.GracefulStop()
			_ = ln.Close()
		})
	}
}

// integrationServiceName returns the service a test registers in, unique per
// test so the tests never see each other's instances.
func integrationServiceName(t *testing.T) string {
	t.Helper()

	return "grpcnacos-it-" + strings.ReplaceAll(t.Name(), "/", "-")
}

// splitAddr splits an address into its host and port.
func splitAddr(t *testing.T, addr string) (string, uint64) {
	t.Helper()

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split address %q: %v", addr, err)
	}
	value, err := strconv.ParseUint(port, 10, 64)
	if err != nil {
		t.Fatalf("parse port of address %q: %v", addr, err)
	}

	return host, value
}

// registerInstance registers the address of a local server as an instance of
// service, and removes it again when the test finishes.
func registerInstance(t *testing.T, n testNacos, service, addr string) Registrar {
	t.Helper()

	host, port := splitAddr(t, addr)
	reg, err := NewRegistrar(n.registrarDsn(t, service, host, port))
	if err != nil {
		t.Fatalf("NewRegistrar(%s) error = %v", addr, err)
	}
	if err := reg.Register(context.Background()); err != nil {
		t.Fatalf("Register(%s) error = %v", addr, err)
	}
	t.Cleanup(func() {
		if err := reg.Deregister(context.Background()); err != nil {
			t.Logf("deregister %s: %v", addr, err)
		}
	})

	return reg
}

// integrationBuilder returns the resolver builder the package registered.
func integrationBuilder(t *testing.T) grpcresolver.Builder {
	t.Helper()

	builder := grpcresolver.Get(scheme)
	if builder == nil {
		t.Fatalf("no resolver builder registered for scheme %q", scheme)
	}

	return builder
}

// resolverTarget builds the resolver target gRPC would build from the same DSN.
func resolverTarget(t *testing.T, n testNacos, service string) grpcresolver.Target {
	t.Helper()

	parsed, err := url.Parse(n.resolverDsn(t, service))
	if err != nil {
		t.Fatalf("parse resolver dsn: %v", err)
	}

	return grpcresolver.Target{URL: *parsed}
}

// healthCheck calls Check for service and maps the reply to a status code: OK
// when the instance served the check, otherwise the code of the call error,
// which it also returns for diagnostics. A health server answers NotFound for
// any service it does not know, which is how the round-trip test tells the two
// instances apart.
func healthCheck(ctx context.Context, client healthpb.HealthClient, service string) (codes.Code, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: service})
	if err != nil {
		return status.Code(err), err
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return codes.Unknown, nil
	}

	return codes.OK, nil
}

// waitForWarmUp polls until every address has served a check at least once,
// which proves the client holds a READY subchannel to each of them.
func waitForWarmUp(t *testing.T, ctx context.Context, client healthpb.HealthClient, addrs []string, timeout time.Duration) {
	t.Helper()

	served := make(map[string]bool, len(addrs))
	seen := map[codes.Code]int{}
	var lastErr error
	deadline := time.Now().Add(timeout)
	for {
		reached := 0
		for _, addr := range addrs {
			if !served[addr] {
				code, err := healthCheck(ctx, client, addr)
				seen[code]++
				if err != nil {
					lastErr = err
				}
				if code == codes.OK {
					served[addr] = true
				}
			}
			if served[addr] {
				reached++
			}
		}
		if reached == len(addrs) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("instances %v were not both reached within %s (reached: %v, codes seen: %v, last error: %v)", addrs, timeout, served, seen, lastErr)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// TestIntegrationRegisterAndDeregister registers the address of a local gRPC
// server in Nacos and watches it appear and disappear there, which the observer
// client reports independently of the code under test.
func TestIntegrationRegisterAndDeregister(t *testing.T) {
	n := newTestNacos(t)
	observer := n.observer(t)
	service := integrationServiceName(t)

	addr, stop := startTestServer(t, "")
	defer stop()

	host, port := splitAddr(t, addr)
	reg, err := NewRegistrar(n.registrarDsn(t, service, host, port))
	if err != nil {
		t.Fatalf("NewRegistrar() error = %v", err)
	}
	if err := reg.Register(context.Background()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	instances := n.waitForInstances(t, observer, service, 1)
	if instances[0].Ip != host || instances[0].Port != port {
		t.Fatalf("registered instance = %s:%d, want %s:%d", instances[0].Ip, instances[0].Port, host, port)
	}

	if err := reg.Deregister(context.Background()); err != nil {
		t.Fatalf("Deregister() error = %v", err)
	}
	n.waitForInstances(t, observer, service, 0)
}

// TestIntegrationResolverPublishesInstances resolves a service with two
// registered instances and checks that the resolver publishes both addresses
// sorted, and drops the deregistered one again.
func TestIntegrationResolverPublishesInstances(t *testing.T) {
	n := newTestNacos(t)
	observer := n.observer(t)
	service := integrationServiceName(t)

	addrA, stopA := startTestServer(t, "")
	defer stopA()
	addrB, stopB := startTestServer(t, "")
	defer stopB()

	registerInstance(t, n, service, addrA)
	regB := registerInstance(t, n, service, addrB)
	n.waitForInstances(t, observer, service, 2)

	conn := newIntegrationConn()
	r, err := integrationBuilder(t).Build(resolverTarget(t, n, service), conn, grpcresolver.BuildOptions{})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	defer r.Close()

	want := []string{addrA, addrB}
	slices.Sort(want)

	state := conn.waitForAddresses(t, 2, pushTimeout)
	if got := addressList(state); !slices.Equal(got, want) {
		t.Fatalf("resolver published %v, want %v", got, want)
	}
	// Nacos carries the grouped service name ("GRPCNACOS@@service") in
	// Instance.ServiceName, so the ServiceName attribute is not asserted here;
	// weight and cluster are the instance values the DSN's defaults produce.
	if got := state.Addresses[0].Attributes.Value("weight"); got != 10.0 {
		t.Fatalf("address attribute weight = %v, want 10", got)
	}
	if got := state.Addresses[0].Attributes.Value("cluster"); got != "DEFAULT" {
		t.Fatalf("address attribute cluster = %v, want %q", got, "DEFAULT")
	}

	if err := regB.Deregister(context.Background()); err != nil {
		t.Fatalf("Deregister(%s) error = %v", addrB, err)
	}

	state = conn.waitForAddresses(t, 1, pushTimeout)
	if got := addressList(state); !slices.Equal(got, []string{addrA}) {
		t.Fatalf("resolver published %v after deregistering %s, want [%s]", got, addrB, addrA)
	}
}

// TestIntegrationResolverUnknownService resolves a service that has no instance
// at all: no address may reach the ClientConn. Nacos reports an empty instance
// list as the error "instance list is empty!", so the resolver either fails the
// build or publishes an empty list; a non-empty list is what must not happen.
func TestIntegrationResolverUnknownService(t *testing.T) {
	n := newTestNacos(t)
	service := integrationServiceName(t)

	conn := newIntegrationConn()
	r, err := integrationBuilder(t).Build(resolverTarget(t, n, service), conn, grpcresolver.BuildOptions{})
	if err != nil {
		t.Logf("Build() for a service without instances = %v", err)
		if state := conn.last(); len(state.Addresses) != 0 {
			t.Fatalf("Build() failed but published %v", addressList(state))
		}

		return
	}
	defer r.Close()

	state := conn.waitForUpdate(t, pushTimeout)
	if len(state.Addresses) != 0 {
		t.Fatalf("resolver published %v for a service without instances", addressList(state))
	}
}

// TestIntegrationGRPCRoundTrip is the end-to-end evidence: a gRPC client
// resolves a Nacos service through the nacos:// resolver, uses both of its
// instances, and keeps serving from the remaining one after the other is
// deregistered.
func TestIntegrationGRPCRoundTrip(t *testing.T) {
	n := newTestNacos(t)
	service := integrationServiceName(t)

	addrA, stopA := startTestServer(t, "")
	defer stopA()
	addrB, stopB := startTestServer(t, "")
	defer stopB()

	registerInstance(t, n, service, addrA)
	regB := registerInstance(t, n, service, addrB)

	// The resolver's own Nacos client must find the instances on its first
	// refresh, otherwise it fails the build with "instance list is empty!";
	// wait until both registrations are visible in Nacos.
	n.waitForInstances(t, n.observer(t), service, 2)

	conn, err := grpc.NewClient(
		n.resolverDsn(t, service),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	defer conn.Close()

	ctx := context.Background()
	client := healthpb.NewHealthClient(conn)

	// A health server reports SERVING only for its own address, so seeing
	// SERVING for both addresses proves both instances are in the address list
	// and reachable before the checks below start.
	waitForWarmUp(t, ctx, client, []string{addrA, addrB}, pushTimeout)

	// With round robin the two instances answer alternately, so asking for one
	// instance's address is served either by that instance (SERVING) or by the
	// other one, which does not know it (NotFound). The checks are grouped per
	// address: round robin rotates once per call, so interleaving the two
	// addresses would send every check of one address to the same instance and
	// it would never be not found.
	serving := map[string]int{}
	missing := map[string]int{}
	for _, addr := range []string{addrA, addrB} {
		for i := 0; i < 20; i++ {
			switch code, _ := healthCheck(ctx, client, addr); code {
			case codes.OK:
				serving[addr]++
			case codes.NotFound:
				missing[addr]++
			default:
				t.Fatalf("Check(%s) = %s, want SERVING or NotFound", addr, code)
			}
		}
		if serving[addr] == 0 || missing[addr] == 0 {
			t.Fatalf("instance %s answered %d checks and was not found %d times, want both", addr, serving[addr], missing[addr])
		}
	}

	// Deregistering an instance has to reach the client: once the resolver has
	// dropped B, every check goes to A, so A keeps serving while B, which has no
	// instance left, is not found any more. Until the address list is refreshed
	// a check can still land on the stopped server, which fails as Unavailable.
	if err := regB.Deregister(context.Background()); err != nil {
		t.Fatalf("Deregister(%s) error = %v", addrB, err)
	}
	stopB()

	stable := 0
	deadline := time.Now().Add(pushTimeout)
	for stable < 3 {
		aCode, _ := healthCheck(ctx, client, addrA)
		bCode, _ := healthCheck(ctx, client, addrB)
		if aCode == codes.OK && bCode == codes.NotFound {
			stable++
			continue
		}

		stable = 0
		if time.Now().After(deadline) {
			t.Fatalf("after deregistering %s: Check(%s) = %s, Check(%s) = %s, want SERVING and NotFound", addrB, addrA, aCode, addrB, bCode)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

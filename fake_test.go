package grpcnacos

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	grpcresolver "google.golang.org/grpc/resolver"
)

// sdkDir is the directory the Nacos SDK log and cache directories live in. It
// is shared and never handed to t.TempDir(): a registrar builds a Nacos client
// through NewRegistrar and Registrar has no Close, so that client keeps writing
// to its log directory after the test that built it ended. Go's TempDir cleanup
// races with that writer and fails the test with "directory not empty".
var sdkDir string

// TestMain shortens the polling fallback once, before any resolver goroutine
// exists. Rewriting defaultPollInterval from a test would race with the poll
// goroutines of the tests that ran before it.
func TestMain(m *testing.M) {
	defaultPollInterval = 10 * time.Millisecond

	dir, err := os.MkdirTemp("", "grpcnacos-sdk-")
	if err != nil {
		panic("create the SDK directory: " + err.Error())
	}
	sdkDir = dir

	code := m.Run()

	// Best effort: a client's log writer may still be alive.
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// fakeNamingClient is a hand written INamingClient double. It embeds the real
// interface so that methods added to the SDK keep compiling, and overrides
// every method this package calls.
type fakeNamingClient struct {
	naming_client.INamingClient

	mu             sync.Mutex
	instances      map[string][]model.Instance
	registered     []vo.RegisterInstanceParam
	deregistered   []vo.DeregisterInstanceParam
	subscribed     *vo.SubscribeParam
	unsubscribed   []vo.SubscribeParam
	selects        int
	installs       int
	closes         int
	registerOK     bool
	deregisterOK   bool
	registerErr    error
	deregisterErr  error
	subscribeErr   error
	selectErr      error
	unsubscribeErr error
}

// newFakeNamingClient returns a double whose registrations and deregistrations
// succeed unless a test says otherwise.
func newFakeNamingClient() *fakeNamingClient {
	return &fakeNamingClient{
		instances:    make(map[string][]model.Instance),
		registerOK:   true,
		deregisterOK: true,
	}
}

// installFakeNamingClient makes every naming client built by the package the
// given double, and restores the real constructor when the test ends.
func installFakeNamingClient(t *testing.T, fake *fakeNamingClient) {
	t.Helper()

	previous := newNamingClient
	newNamingClient = func(vo.NacosClientParam) (naming_client.INamingClient, error) {
		fake.mu.Lock()
		fake.installs++
		fake.mu.Unlock()
		return fake, nil
	}
	t.Cleanup(func() { newNamingClient = previous })
}

// installCount reports how many naming clients the package asked for.
func (f *fakeNamingClient) installCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.installs
}

// setInstances replaces the instance list SelectInstances returns, keyed by
// group and service exactly like a Nacos service.
func (f *fakeNamingClient) setInstances(group, service string, instances ...model.Instance) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(instances) == 0 {
		f.instances[instanceKey(group, service)] = nil
		return
	}
	f.instances[instanceKey(group, service)] = append([]model.Instance(nil), instances...)
}

// push makes the subscription the package installed see a fresh instance list,
// like a Nacos push, and records it as the new server side truth.
func (f *fakeNamingClient) push(instances ...model.Instance) bool {
	f.mu.Lock()
	param := f.subscribed
	if param == nil {
		f.mu.Unlock()
		return false
	}
	f.instances[instanceKey(param.GroupName, param.ServiceName)] = append([]model.Instance(nil), instances...)
	callback := param.SubscribeCallback
	f.mu.Unlock()

	callback(instances, nil)
	return true
}

func (f *fakeNamingClient) registerCalls() []vo.RegisterInstanceParam {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]vo.RegisterInstanceParam(nil), f.registered...)
}

func (f *fakeNamingClient) deregisterCalls() []vo.DeregisterInstanceParam {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]vo.DeregisterInstanceParam(nil), f.deregistered...)
}

func (f *fakeNamingClient) unsubscribeCalls() []vo.SubscribeParam {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]vo.SubscribeParam(nil), f.unsubscribed...)
}

func (f *fakeNamingClient) selectCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.selects
}

func (f *fakeNamingClient) closeCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closes
}

func (f *fakeNamingClient) isClosed() bool {
	return f.closeCalls() > 0
}

func (f *fakeNamingClient) RegisterInstance(param vo.RegisterInstanceParam) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registered = append(f.registered, param)
	return f.registerOK, f.registerErr
}

func (f *fakeNamingClient) DeregisterInstance(param vo.DeregisterInstanceParam) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deregistered = append(f.deregistered, param)
	return f.deregisterOK, f.deregisterErr
}

func (f *fakeNamingClient) SelectInstances(param vo.SelectInstancesParam) ([]model.Instance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.selects++
	if f.selectErr != nil {
		return nil, f.selectErr
	}
	return append([]model.Instance(nil), f.instances[instanceKey(param.GroupName, param.ServiceName)]...), nil
}

func (f *fakeNamingClient) Subscribe(param *vo.SubscribeParam) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subscribed = param
	return f.subscribeErr
}

func (f *fakeNamingClient) Unsubscribe(param *vo.SubscribeParam) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if param != nil {
		f.unsubscribed = append(f.unsubscribed, *param)
	}
	return f.unsubscribeErr
}

func (f *fakeNamingClient) CloseClient() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closes++
}

// Methods this package never calls; they return zero values so the double
// still satisfies the interface.

func (f *fakeNamingClient) BatchRegisterInstance(vo.BatchRegisterInstanceParam) (bool, error) {
	return false, nil
}

func (f *fakeNamingClient) UpdateInstance(vo.UpdateInstanceParam) (bool, error) {
	return false, nil
}

func (f *fakeNamingClient) GetService(vo.GetServiceParam) (model.Service, error) {
	return model.Service{}, nil
}

func (f *fakeNamingClient) SelectAllInstances(vo.SelectAllInstancesParam) ([]model.Instance, error) {
	return nil, nil
}

func (f *fakeNamingClient) SelectOneHealthyInstance(vo.SelectOneHealthInstanceParam) (*model.Instance, error) {
	return nil, nil
}

func (f *fakeNamingClient) GetAllServicesInfo(vo.GetAllServiceInfoParam) (model.ServiceList, error) {
	return model.ServiceList{}, nil
}

func (f *fakeNamingClient) ServerHealthy() bool {
	return true
}

func instanceKey(group, service string) string {
	return group + "\x00" + service
}

// instance is a healthy, enabled Nacos instance as the resolver expects it.
func instance(ip string, port uint64) model.Instance {
	return model.Instance{
		ServiceName: "svc",
		Ip:          ip,
		Port:        port,
		ClusterName: "DEFAULT",
		Weight:      10,
		Healthy:     true,
		Enable:      true,
		Metadata:    map[string]string{"zone": "a"},
	}
}

// fakeConn records the states the resolver reports.
type fakeConn struct {
	grpcresolver.ClientConn

	mu     sync.Mutex
	states []grpcresolver.State
	errs   []error
}

func newFakeConn() *fakeConn {
	return &fakeConn{}
}

func (c *fakeConn) UpdateState(state grpcresolver.State) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.states = append(c.states, state)
	return nil
}

func (c *fakeConn) ReportError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs = append(c.errs, err)
}

// lastState returns the most recent state, and whether one was reported at all.
func (c *fakeConn) lastState() (grpcresolver.State, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.states) == 0 {
		return grpcresolver.State{}, false
	}
	return c.states[len(c.states)-1], true
}

// stateCount reports how many states were reported.
func (c *fakeConn) stateCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.states)
}

// waitForAddresses blocks until the last reported state holds want addresses.
func (c *fakeConn) waitForAddresses(t *testing.T, want int, timeout time.Duration) []grpcresolver.Address {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		if state, ok := c.lastState(); ok && len(state.Addresses) == want {
			return state.Addresses
		}
		if time.Now().After(deadline) {
			state, _ := c.lastState()
			t.Fatalf("resolver state has %d addresses, want %d (state %+v)", len(state.Addresses), want, state)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// addresses returns the addresses of the last reported state.
func (c *fakeConn) addresses(t *testing.T) []grpcresolver.Address {
	t.Helper()

	state, ok := c.lastState()
	if !ok {
		t.Fatal("resolver reported no state")
	}
	return state.Addresses
}

// addressStrings flattens addresses into their "host:port" form.
func addressStrings(addrs []grpcresolver.Address) []string {
	out := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		out = append(out, addr.Addr)
	}
	return out
}

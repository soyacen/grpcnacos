package grpcnacos

import (
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpcresolver "google.golang.org/grpc/resolver"
)

const resolverDsn = "nacos://127.0.0.1:8848/svc"

func buildTarget(t *testing.T, dsn string) grpcresolver.Target {
	t.Helper()

	u, err := url.Parse(dsn)
	require.NoError(t, err)
	return grpcresolver.Target{URL: *u}
}

func builder(t *testing.T) grpcresolver.Builder {
	t.Helper()

	b := grpcresolver.Get(scheme)
	require.NotNil(t, b, "init() must register the %q resolver", scheme)
	return b
}

// buildResolver builds a resolver for dsn and closes it when the test ends.
func buildResolver(t *testing.T, conn grpcresolver.ClientConn, dsn string) {
	t.Helper()

	r, err := builder(t).Build(buildTarget(t, dsn), conn, grpcresolver.BuildOptions{})
	require.NoError(t, err)
	t.Cleanup(r.Close)
}

func TestBuilderScheme(t *testing.T) {
	assert.Equal(t, "nacos", (&Builder{}).Scheme())

	b := grpcresolver.Get("nacos")
	require.NotNil(t, b)
	assert.IsType(t, &Builder{}, b)
}

func TestBuildInvalidDsn(t *testing.T) {
	fake := newFakeNamingClient()
	installFakeNamingClient(t, fake)
	conn := newFakeConn()

	r, err := builder(t).Build(buildTarget(t, "http://127.0.0.1:8848/svc"), conn, grpcresolver.BuildOptions{})

	require.Error(t, err)
	assert.Nil(t, r)
	assert.Equal(t, 0, fake.installCount(), "no client may be built for a DSN that does not parse")
	assert.Equal(t, 0, conn.stateCount())
}

func TestBuildPublishesSortedHealthyAddresses(t *testing.T) {
	fake := newFakeNamingClient()
	installFakeNamingClient(t, fake)
	unhealthy := instance("10.0.0.9", 9109)
	unhealthy.Healthy = false
	fake.setInstances(defaultGroup, "svc",
		instance("10.0.0.2", 9102),
		instance("10.0.0.1", 9101),
		unhealthy,
	)
	conn := newFakeConn()

	buildResolver(t, conn, resolverDsn)

	addrs := conn.addresses(t)
	require.Len(t, addrs, 2)
	assert.Equal(t, []string{"10.0.0.1:9101", "10.0.0.2:9102"}, addressStrings(addrs))

	require.NotNil(t, addrs[0].Attributes)
	for key, want := range map[string]any{
		"ServiceName": "svc",
		"weight":      10.0,
		"cluster":     "DEFAULT",
		"zone":        "a",
	} {
		assert.Equal(t, want, addrs[0].Attributes.Value(key), "address attribute %q", key)
	}
}

func TestBuildEmptyService(t *testing.T) {
	fake := newFakeNamingClient()
	installFakeNamingClient(t, fake)
	fake.setInstances(defaultGroup, "svc")
	conn := newFakeConn()

	buildResolver(t, conn, resolverDsn)

	state, ok := conn.lastState()
	require.True(t, ok, "an empty instance list must still be reported")
	require.NotNil(t, state.Addresses)
	assert.Empty(t, state.Addresses)
}

func TestSubscriptionPushUpdatesState(t *testing.T) {
	fake := newFakeNamingClient()
	installFakeNamingClient(t, fake)
	fake.setInstances(defaultGroup, "svc", instance("10.0.0.1", 9101))
	conn := newFakeConn()

	buildResolver(t, conn, resolverDsn)
	require.Equal(t, []string{"10.0.0.1:9101"}, addressStrings(conn.addresses(t)))

	require.True(t, fake.push(instance("10.0.0.3", 9103)))
	assert.Equal(t, []string{"10.0.0.3:9103"}, addressStrings(conn.addresses(t)))
}

func TestBuildFailsOnSubscribeError(t *testing.T) {
	wantErr := errors.New("subscribe failed")
	fake := newFakeNamingClient()
	fake.subscribeErr = wantErr
	installFakeNamingClient(t, fake)
	conn := newFakeConn()

	r, err := builder(t).Build(buildTarget(t, resolverDsn), conn, grpcresolver.BuildOptions{})

	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, r)
	assert.True(t, fake.isClosed(), "the client must not leak when the resolver fails to start")
	assert.Equal(t, 0, fake.selectCalls())
}

func TestBuildFailsOnSelectError(t *testing.T) {
	wantErr := errors.New("select failed")
	fake := newFakeNamingClient()
	fake.selectErr = wantErr
	installFakeNamingClient(t, fake)
	conn := newFakeConn()

	r, err := builder(t).Build(buildTarget(t, resolverDsn), conn, grpcresolver.BuildOptions{})

	require.ErrorIs(t, err, wantErr)
	assert.Nil(t, r)
	assert.True(t, fake.isClosed(), "the client must not leak when the resolver fails to start")
	assert.Len(t, fake.unsubscribeCalls(), 1, "the live subscription must be taken back")
}

func TestPollFallbackRefreshes(t *testing.T) {
	fake := newFakeNamingClient()
	installFakeNamingClient(t, fake)
	fake.setInstances(defaultGroup, "svc", instance("10.0.0.1", 9101))
	conn := newFakeConn()

	buildResolver(t, conn, resolverDsn)
	require.Equal(t, []string{"10.0.0.1:9101"}, addressStrings(conn.addresses(t)))

	// The subscription pushes nothing here: only the polling fallback can pick
	// the new instance up.
	fake.setInstances(defaultGroup, "svc", instance("10.0.0.1", 9101), instance("10.0.0.2", 9102))

	assert.Equal(t, []string{"10.0.0.1:9101", "10.0.0.2:9102"}, addressStrings(conn.waitForAddresses(t, 2, time.Second)))
}

func TestCloseStopsPollingAndIsIdempotent(t *testing.T) {
	fake := newFakeNamingClient()
	installFakeNamingClient(t, fake)
	fake.setInstances(defaultGroup, "svc", instance("10.0.0.1", 9101))
	conn := newFakeConn()

	r, err := builder(t).Build(buildTarget(t, resolverDsn), conn, grpcresolver.BuildOptions{})
	require.NoError(t, err)

	r.Close()
	r.Close()

	assert.Equal(t, 1, fake.closeCalls(), "Close must close the client exactly once")
	assert.Len(t, fake.unsubscribeCalls(), 1, "Close must unsubscribe exactly once")

	before := conn.stateCount()
	fake.setInstances(defaultGroup, "svc",
		instance("10.0.0.1", 9101),
		instance("10.0.0.2", 9102),
		instance("10.0.0.3", 9103),
	)
	time.Sleep(200 * time.Millisecond)

	assert.Equal(t, before, conn.stateCount(), "polling must stop after Close")
	assert.Equal(t, []string{"10.0.0.1:9101"}, addressStrings(conn.addresses(t)))
}

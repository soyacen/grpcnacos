package grpcnacos

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const registrarDsn = "nacos://127.0.0.1:8848/svc?ip=1.2.3.4&port=9090"

func TestNewRegistrar(t *testing.T) {
	fake := newFakeNamingClient()
	installFakeNamingClient(t, fake)

	reg, err := NewRegistrar(registrarDsn)
	require.NoError(t, err)
	require.NotNil(t, reg)
	assert.Empty(t, fake.registerCalls(), "creating a registrar must not register anything")
}

func TestNewRegistrarInvalidDsn(t *testing.T) {
	fake := newFakeNamingClient()
	installFakeNamingClient(t, fake)

	reg, err := NewRegistrar("nacos://127.0.0.1:8848/svc")

	require.Error(t, err)
	assert.Nil(t, reg)
	assert.Equal(t, 0, fake.installCount(), "no client may be built for a DSN that does not parse")
}

func TestRegistrarRegister(t *testing.T) {
	fake := newFakeNamingClient()
	installFakeNamingClient(t, fake)

	reg, err := NewRegistrar(registrarDsn)
	require.NoError(t, err)
	require.NoError(t, reg.Register(context.Background()))

	calls := fake.registerCalls()
	require.Len(t, calls, 1)
	assert.Equal(t, "1.2.3.4", calls[0].Ip)
	assert.Equal(t, uint64(9090), calls[0].Port)
	assert.Equal(t, "svc", calls[0].ServiceName)
	assert.Equal(t, "DEFAULT_GROUP", calls[0].GroupName)
	assert.Equal(t, 10.0, calls[0].Weight)
	assert.True(t, calls[0].Enable)
	assert.True(t, calls[0].Healthy)
	assert.True(t, calls[0].Ephemeral)
}

func TestRegistrarDeregister(t *testing.T) {
	fake := newFakeNamingClient()
	installFakeNamingClient(t, fake)

	reg, err := NewRegistrar(registrarDsn)
	require.NoError(t, err)
	require.NoError(t, reg.Deregister(context.Background()))

	calls := fake.deregisterCalls()
	require.Len(t, calls, 1)
	assert.Equal(t, "1.2.3.4", calls[0].Ip)
	assert.Equal(t, uint64(9090), calls[0].Port)
	assert.Equal(t, "svc", calls[0].ServiceName)
	assert.Equal(t, "DEFAULT_GROUP", calls[0].GroupName)
	assert.True(t, calls[0].Ephemeral)
}

func TestRegistrarRegisterFailed(t *testing.T) {
	t.Run("not ok", func(t *testing.T) {
		fake := newFakeNamingClient()
		fake.registerOK = false
		installFakeNamingClient(t, fake)

		reg, err := NewRegistrar(registrarDsn)
		require.NoError(t, err)

		err = reg.Register(context.Background())
		require.Error(t, err)
		assert.Equal(t, "grpcnacos: failed to register svc", err.Error())
	})

	t.Run("client error", func(t *testing.T) {
		wantErr := errors.New("nacos is down")
		fake := newFakeNamingClient()
		fake.registerErr = wantErr
		installFakeNamingClient(t, fake)

		reg, err := NewRegistrar(registrarDsn)
		require.NoError(t, err)

		err = reg.Register(context.Background())
		assert.ErrorIs(t, err, wantErr)
		assert.False(t, strings.HasPrefix(err.Error(), "grpcnacos:"), "a client error must be returned unwrapped")
	})
}

func TestRegistrarDeregisterFailed(t *testing.T) {
	t.Run("not ok", func(t *testing.T) {
		fake := newFakeNamingClient()
		fake.deregisterOK = false
		installFakeNamingClient(t, fake)

		reg, err := NewRegistrar(registrarDsn)
		require.NoError(t, err)

		err = reg.Deregister(context.Background())
		require.Error(t, err)
		assert.Equal(t, "grpcnacos: failed to deregister svc", err.Error())
	})

	t.Run("client error", func(t *testing.T) {
		wantErr := errors.New("nacos is down")
		fake := newFakeNamingClient()
		fake.deregisterErr = wantErr
		installFakeNamingClient(t, fake)

		reg, err := NewRegistrar(registrarDsn)
		require.NoError(t, err)

		err = reg.Deregister(context.Background())
		assert.ErrorIs(t, err, wantErr)
	})
}

func TestNacosFactoryNew(t *testing.T) {
	fake := newFakeNamingClient()
	installFakeNamingClient(t, fake)

	reg, err := (&nacosFactory{}).New(context.Background(), registrarDsn)
	require.NoError(t, err)
	require.NoError(t, reg.Register(context.Background()))
	assert.Len(t, fake.registerCalls(), 1)

	reg, err = (&nacosFactory{}).New(context.Background(), "http://127.0.0.1:8848/svc")
	require.Error(t, err)
	assert.Nil(t, reg)
}

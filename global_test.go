package grpcnacos

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegisterAndGet(t *testing.T) {
	name := strings.ToUpper(t.Name())
	resource := &nacosFactory{}
	Register(name, resource)

	for _, lookup := range []string{strings.ToLower(name), strings.ToUpper(name), name} {
		got, ok := Get(lookup)
		require.True(t, ok, "Get(%q) must find the factory", lookup)
		assert.Same(t, resource, got)
	}

	_, ok := Get("grpcnacos-no-such-factory")
	assert.False(t, ok)
}

func TestRegisterNil(t *testing.T) {
	assert.PanicsWithValue(t, "grpcnacos: Register resource is nil", func() {
		Register(strings.ToUpper(t.Name()), nil)
	})
}

func TestRegisterDuplicate(t *testing.T) {
	name := strings.ToUpper(t.Name())
	Register(name, &nacosFactory{})

	assert.PanicsWithValue(t,
		"grpcnacos: Register called twice for resource "+strings.ToLower(name),
		func() { Register(name, &nacosFactory{}) })
}

func TestNacosFactoryRegistered(t *testing.T) {
	resource, ok := Get("nacos")
	require.True(t, ok, "init() must register the nacos factory")
	assert.IsType(t, &nacosFactory{}, resource)
}

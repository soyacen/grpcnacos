package grpcnacos

import (
	"context"
	"strings"
	"testing"

	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDsnRegistrarDefaults(t *testing.T) {
	parsed, err := ParseDsn(context.Background(), kindRegistrar, "nacos://127.0.0.1:8848/svc?ip=1.2.3.4&port=9090")
	require.NoError(t, err)

	assert.Equal(t, "public", parsed.ClientParam.ClientConfig.NamespaceId)
	assert.Equal(t, uint64(10000), parsed.ClientParam.ClientConfig.TimeoutMs)
	assert.True(t, parsed.ClientParam.ClientConfig.NotLoadCacheAtStart)
	assert.Empty(t, parsed.ClientParam.ClientConfig.Username)
	assert.Empty(t, parsed.ClientParam.ClientConfig.Password)

	require.Len(t, parsed.ClientParam.ServerConfigs, 1)
	assert.Equal(t, "127.0.0.1", parsed.ClientParam.ServerConfigs[0].IpAddr)
	assert.Equal(t, uint64(8848), parsed.ClientParam.ServerConfigs[0].Port)

	assert.Equal(t, vo.RegisterInstanceParam{
		Ip:          "1.2.3.4",
		Port:        9090,
		Weight:      10,
		Enable:      true,
		Healthy:     true,
		Metadata:    map[string]string{},
		ServiceName: "svc",
		GroupName:   "DEFAULT_GROUP",
		Ephemeral:   true,
	}, parsed.RegisterParam)
	assert.Equal(t, vo.DeregisterInstanceParam{
		Ip:          "1.2.3.4",
		Port:        9090,
		ServiceName: "svc",
		GroupName:   "DEFAULT_GROUP",
		Ephemeral:   true,
	}, parsed.DeregisterParam)
}

func TestParseDsnRegistrarAllParameters(t *testing.T) {
	dsn := "nacos://user:pass@10.1.2.3:8849/svc" +
		"?namespace=ns1&group=grp1&timeout=5000" +
		"&logDir=/tmp/log&cacheDir=/tmp/cache&logLevel=debug&notLoadCacheAtStart=false" +
		"&ip=1.2.3.4&port=9090&weight=20&ephemeral=false&cluster=c1&meta.zone=z1&meta.env=prod"

	parsed, err := ParseDsn(context.Background(), kindRegistrar, dsn)
	require.NoError(t, err)

	assert.Equal(t, "user", parsed.ClientParam.ClientConfig.Username)
	assert.Equal(t, "pass", parsed.ClientParam.ClientConfig.Password)
	assert.Equal(t, "ns1", parsed.ClientParam.ClientConfig.NamespaceId)
	assert.Equal(t, uint64(5000), parsed.ClientParam.ClientConfig.TimeoutMs)
	assert.Equal(t, "/tmp/log", parsed.ClientParam.ClientConfig.LogDir)
	assert.Equal(t, "/tmp/cache", parsed.ClientParam.ClientConfig.CacheDir)
	assert.Equal(t, "debug", parsed.ClientParam.ClientConfig.LogLevel)
	assert.False(t, parsed.ClientParam.ClientConfig.NotLoadCacheAtStart)

	require.Len(t, parsed.ClientParam.ServerConfigs, 1)
	assert.Equal(t, "10.1.2.3", parsed.ClientParam.ServerConfigs[0].IpAddr)
	assert.Equal(t, uint64(8849), parsed.ClientParam.ServerConfigs[0].Port)

	assert.Equal(t, vo.RegisterInstanceParam{
		Ip:          "1.2.3.4",
		Port:        9090,
		Weight:      20,
		Enable:      true,
		Healthy:     true,
		Metadata:    map[string]string{"zone": "z1", "env": "prod"},
		ClusterName: "c1",
		ServiceName: "svc",
		GroupName:   "grp1",
		Ephemeral:   false,
	}, parsed.RegisterParam)
	assert.Equal(t, vo.DeregisterInstanceParam{
		Ip:          "1.2.3.4",
		Port:        9090,
		Cluster:     "c1",
		ServiceName: "svc",
		GroupName:   "grp1",
		Ephemeral:   false,
	}, parsed.DeregisterParam)
}

func TestParseDsnRegistrarDefaultPort(t *testing.T) {
	parsed, err := ParseDsn(context.Background(), kindRegistrar, "nacos://127.0.0.1/svc?ip=1.2.3.4&port=9090")
	require.NoError(t, err)

	require.Len(t, parsed.ClientParam.ServerConfigs, 1)
	assert.Equal(t, uint64(defaultPort), parsed.ClientParam.ServerConfigs[0].Port)
}

func TestParseDsnResolver(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		parsed, err := ParseDsn(context.Background(), kindResolver, "nacos://127.0.0.1:8848/svc")
		require.NoError(t, err)

		assert.Equal(t, "public", parsed.ClientParam.ClientConfig.NamespaceId)
		assert.Equal(t, uint64(10000), parsed.ClientParam.ClientConfig.TimeoutMs)
		assert.True(t, parsed.ClientParam.ClientConfig.NotLoadCacheAtStart)

		assert.Equal(t, vo.SubscribeParam{
			ServiceName: "svc",
			GroupName:   "DEFAULT_GROUP",
		}, parsed.SubscribeParam)
		assert.Equal(t, vo.RegisterInstanceParam{}, parsed.RegisterParam)
		assert.Equal(t, vo.DeregisterInstanceParam{}, parsed.DeregisterParam)
	})

	t.Run("all parameters", func(t *testing.T) {
		dsn := "nacos://user:pass@127.0.0.1:8848/svc" +
			"?namespace=ns1&group=grp1&timeout=5000&clusters=c1,c2" +
			"&logDir=/tmp/log&cacheDir=/tmp/cache&logLevel=warn&notLoadCacheAtStart=false"

		parsed, err := ParseDsn(context.Background(), kindResolver, dsn)
		require.NoError(t, err)

		assert.Equal(t, "user", parsed.ClientParam.ClientConfig.Username)
		assert.Equal(t, "pass", parsed.ClientParam.ClientConfig.Password)
		assert.Equal(t, "ns1", parsed.ClientParam.ClientConfig.NamespaceId)
		assert.Equal(t, uint64(5000), parsed.ClientParam.ClientConfig.TimeoutMs)
		assert.Equal(t, "/tmp/log", parsed.ClientParam.ClientConfig.LogDir)
		assert.Equal(t, "/tmp/cache", parsed.ClientParam.ClientConfig.CacheDir)
		assert.Equal(t, "warn", parsed.ClientParam.ClientConfig.LogLevel)
		assert.False(t, parsed.ClientParam.ClientConfig.NotLoadCacheAtStart)

		assert.Equal(t, vo.SubscribeParam{
			ServiceName: "svc",
			GroupName:   "grp1",
			Clusters:    []string{"c1", "c2"},
		}, parsed.SubscribeParam)
	})
}

func TestParseDsnClusters(t *testing.T) {
	tests := []struct {
		name     string
		dsn      string
		expected []string
	}{
		{
			name:     "absent stays nil",
			dsn:      "nacos://127.0.0.1:8848/svc",
			expected: nil,
		},
		{
			name:     "comma separated and trimmed",
			dsn:      "nacos://127.0.0.1:8848/svc?clusters=c1, c2",
			expected: []string{"c1", "c2"},
		},
		{
			name:     "blank entries dropped",
			dsn:      "nacos://127.0.0.1:8848/svc?clusters=,c1,,",
			expected: []string{"c1"},
		},
		{
			name:     "empty value stays nil",
			dsn:      "nacos://127.0.0.1:8848/svc?clusters=",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := ParseDsn(context.Background(), kindResolver, tt.dsn)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, parsed.SubscribeParam.Clusters)
		})
	}
}

func TestParseDsnErrors(t *testing.T) {
	tests := []struct {
		name         string
		kind         string
		dsn          string
		wantContains string
	}{
		{name: "empty dsn", kind: kindRegistrar, dsn: "", wantContains: "unsupported scheme"},
		{name: "unsupported scheme", kind: kindRegistrar, dsn: "http://127.0.0.1:8848/svc?ip=1.2.3.4&port=9090", wantContains: "unsupported scheme"},
		{name: "missing host", kind: kindResolver, dsn: "nacos:///svc", wantContains: "has no host"},
		{name: "malformed port", kind: kindResolver, dsn: "nacos://127.0.0.1:port/svc", wantContains: "grpcnacos:"},
		{name: "missing service name", kind: kindResolver, dsn: "nacos://127.0.0.1:8848", wantContains: "service name is required"},
		{name: "invalid timeout", kind: kindResolver, dsn: "nacos://127.0.0.1:8848/svc?timeout=abc", wantContains: "invalid timeout"},
		{name: "invalid notLoadCacheAtStart", kind: kindResolver, dsn: "nacos://127.0.0.1:8848/svc?notLoadCacheAtStart=yes", wantContains: "invalid notLoadCacheAtStart"},
		{name: "registrar without ip", kind: kindRegistrar, dsn: "nacos://127.0.0.1:8848/svc?port=9090", wantContains: "service ip is required"},
		{name: "registrar with invalid port", kind: kindRegistrar, dsn: "nacos://127.0.0.1:8848/svc?ip=1.2.3.4&port=abc", wantContains: "invalid service port"},
		{name: "registrar with invalid weight", kind: kindRegistrar, dsn: "nacos://127.0.0.1:8848/svc?ip=1.2.3.4&port=9090&weight=abc", wantContains: "invalid weight"},
		{name: "registrar with invalid ephemeral", kind: kindRegistrar, dsn: "nacos://127.0.0.1:8848/svc?ip=1.2.3.4&port=9090&ephemeral=abc", wantContains: "invalid ephemeral"},
		{name: "unsupported kind", kind: "wat", dsn: "nacos://127.0.0.1:8848/svc", wantContains: "unsupported kind"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := ParseDsn(context.Background(), tt.kind, tt.dsn)
			require.Error(t, err)
			assert.Nil(t, parsed)
			assert.True(t, strings.HasPrefix(err.Error(), "grpcnacos:"), "error %q must be prefixed", err)
			assert.Contains(t, err.Error(), tt.wantContains)
		})
	}
}

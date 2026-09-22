package grpcnacos

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// Default values applied when a DSN does not spell a parameter out.
const (
	defaultPort      uint64 = 8848
	defaultNamespace        = "public"
	defaultGroup            = "DEFAULT_GROUP"
	defaultTimeoutMs uint64 = 10000
	defaultWeight           = 10.0
)

// The kinds of DSN ParseDsn understands.
const (
	kindRegistrar = "registrar"
	kindResolver  = "resolver"
)

// DSN is a parsed DSN: the Nacos client parameters plus the parameters of the
// service the DSN refers to.
type DSN struct {
	ClientParam     vo.NacosClientParam        // Nacos client parameters
	RegisterParam   vo.RegisterInstanceParam   // Registration instance parameters
	DeregisterParam vo.DeregisterInstanceParam // Deregistration instance parameters
	SubscribeParam  vo.SubscribeParam          // Subscription parameters
}

// DsnParser parses a DSN of the given kind ("registrar" or "resolver").
type DsnParser func(ctx context.Context, kind string, dsn string) (*DSN, error)

// DefaultDsnParser is the DSN parser used by NewRegistrar and Builder.Build.
var DefaultDsnParser DsnParser = ParseDsn

// ParseDsn parses a DSN of the form
//
//	nacos://[username[:password]@]host[:port]/service_name?param=value
//
// kind selects which parameters are required and which part of the DSN gets
// filled in: "registrar" needs ip and port, "resolver" accepts clusters. The
// returned DSN always carries the Nacos client parameters.
func ParseDsn(ctx context.Context, kind string, dsn string) (*DSN, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("grpcnacos: parse dsn %q: %w", dsn, err)
	}
	if u.Scheme != scheme {
		return nil, fmt.Errorf("grpcnacos: unsupported scheme %q in dsn %q, only %q is supported", u.Scheme, dsn, scheme)
	}
	host := u.Hostname()
	if host == "" {
		return nil, fmt.Errorf("grpcnacos: dsn %q has no host", dsn)
	}
	port := defaultPort
	if p := u.Port(); p != "" {
		port, err = strconv.ParseUint(p, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("grpcnacos: invalid port %q in dsn %q: %w", p, dsn, err)
		}
	}

	serviceName := strings.Trim(u.Path, "/")
	if serviceName == "" {
		return nil, fmt.Errorf("grpcnacos: service name is required in path of dsn %q", dsn)
	}

	// Parse query parameters.
	q := u.Query()
	namespace := q.Get("namespace")
	if namespace == "" {
		namespace = defaultNamespace
	}
	group := q.Get("group")
	if group == "" {
		group = defaultGroup
	}

	timeoutMs := defaultTimeoutMs
	if t := q.Get("timeout"); t != "" {
		v, err := strconv.ParseUint(t, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("grpcnacos: invalid timeout %q in dsn %q: %w", t, dsn, err)
		}
		timeoutMs = v
	}

	notLoadCacheAtStart := true
	if t := q.Get("notLoadCacheAtStart"); t != "" {
		v, err := strconv.ParseBool(t)
		if err != nil {
			return nil, fmt.Errorf("grpcnacos: invalid notLoadCacheAtStart %q in dsn %q: %w", t, dsn, err)
		}
		notLoadCacheAtStart = v
	}

	// Construct the client configuration.
	clientConfig := constant.NewClientConfig()
	clientConfig.Username = u.User.Username()
	clientConfig.Password, _ = u.User.Password()
	clientConfig.NamespaceId = namespace
	clientConfig.TimeoutMs = timeoutMs
	clientConfig.NotLoadCacheAtStart = notLoadCacheAtStart
	if v := q.Get("logDir"); v != "" {
		clientConfig.LogDir = v
	}
	if v := q.Get("cacheDir"); v != "" {
		clientConfig.CacheDir = v
	}
	if v := q.Get("logLevel"); v != "" {
		clientConfig.LogLevel = v
	}

	clientParam := vo.NacosClientParam{
		ClientConfig: clientConfig,
		ServerConfigs: []constant.ServerConfig{
			*constant.NewServerConfig(host, port),
		},
	}

	d := &DSN{ClientParam: clientParam}

	switch kind {
	case kindRegistrar:
		// Parse instance IP and port.
		svcIP := q.Get("ip")
		if svcIP == "" {
			return nil, fmt.Errorf("grpcnacos: service ip is required in query parameters of dsn %q", dsn)
		}
		svcPortStr := q.Get("port")
		svcPort, err := strconv.ParseUint(svcPortStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("grpcnacos: invalid service port %q in dsn %q: %w", svcPortStr, dsn, err)
		}

		// Parse weight configuration.
		weight := defaultWeight
		if w := q.Get("weight"); w != "" {
			v, err := strconv.ParseFloat(w, 64)
			if err != nil {
				return nil, fmt.Errorf("grpcnacos: invalid weight %q in dsn %q: %w", w, dsn, err)
			}
			weight = v
		}

		// Parse ephemeral instance configuration.
		ephemeral := true
		if e := q.Get("ephemeral"); e != "" {
			v, err := strconv.ParseBool(e)
			if err != nil {
				return nil, fmt.Errorf("grpcnacos: invalid ephemeral %q in dsn %q: %w", e, dsn, err)
			}
			ephemeral = v
		}

		// Parse cluster name and metadata.
		cluster := q.Get("cluster")
		metadata := make(map[string]string)
		for k, v := range q {
			if strings.HasPrefix(k, "meta.") {
				metadata[strings.TrimPrefix(k, "meta.")] = v[0]
			}
		}

		d.RegisterParam = vo.RegisterInstanceParam{
			Ip:          svcIP,
			Port:        svcPort,
			Weight:      weight,
			Enable:      true, // Default enabled
			Healthy:     true, // Default healthy
			Metadata:    metadata,
			ClusterName: cluster,
			ServiceName: serviceName,
			GroupName:   group,
			Ephemeral:   ephemeral,
		}
		d.DeregisterParam = vo.DeregisterInstanceParam{
			Ip:          svcIP,
			Port:        svcPort,
			Cluster:     cluster,
			ServiceName: serviceName,
			GroupName:   group,
			Ephemeral:   ephemeral,
		}
	case kindResolver:
		// An absent clusters parameter must stay nil: Nacos treats an empty
		// cluster name as a cluster of its own.
		var clusters []string
		for _, c := range strings.Split(q.Get("clusters"), ",") {
			if c = strings.TrimSpace(c); c != "" {
				clusters = append(clusters, c)
			}
		}

		d.SubscribeParam = vo.SubscribeParam{
			ServiceName: serviceName,
			GroupName:   group,
			Clusters:    clusters,
		}
	default:
		return nil, fmt.Errorf("grpcnacos: unsupported kind %q for dsn %q, only %q and %q are supported", kind, dsn, kindRegistrar, kindResolver)
	}

	return d, nil
}

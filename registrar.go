package grpcnacos

import (
	"context"
	"fmt"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

var _ Registrar = (*registrar)(nil)

// newNamingClient builds a Nacos naming client; a variable so tests can substitute a fake.
var newNamingClient = clients.NewNamingClient

// init registers the nacos factory in the package registry.
func init() {
	Register(scheme, &nacosFactory{})
}

// registrar represents the Nacos service registrar structure
type registrar struct {
	namingClient    naming_client.INamingClient // Nacos naming client
	registerParam   vo.RegisterInstanceParam    // Registration instance parameters
	deregisterParam vo.DeregisterInstanceParam  // Deregistration instance parameters
}

// Register registers a service instance in Nacos.
func (r *registrar) Register(ctx context.Context) error {
	ok, err := r.namingClient.RegisterInstance(r.registerParam)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("grpcnacos: failed to register %s", r.registerParam.ServiceName)
	}

	return nil
}

// Deregister deregisters a service instance from Nacos.
func (r *registrar) Deregister(ctx context.Context) error {
	ok, err := r.namingClient.DeregisterInstance(r.deregisterParam)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("grpcnacos: failed to deregister %s", r.deregisterParam.ServiceName)
	}

	return nil
}

// NewRegistrar creates a new Nacos service registrar for dsn, which has the
// form nacos://[username[:password]@]host[:port]/service_name?ip=<ip>&port=<port>.
func NewRegistrar(dsn string) (Registrar, error) {
	parsed, err := DefaultDsnParser(context.Background(), kindRegistrar, dsn)
	if err != nil {
		return nil, err
	}

	client, err := newNamingClient(parsed.ClientParam)
	if err != nil {
		return nil, err
	}

	return &registrar{
		namingClient:    client,
		registerParam:   parsed.RegisterParam,
		deregisterParam: parsed.DeregisterParam,
	}, nil
}

// nacosFactory builds registrars from nacos:// DSNs.
type nacosFactory struct{}

// New creates a new registrar instance for dsn.
func (f *nacosFactory) New(ctx context.Context, dsn string) (Registrar, error) {
	return NewRegistrar(dsn)
}

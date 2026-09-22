package grpcnacos

import (
	"cmp"
	"context"
	"net"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"google.golang.org/grpc/attributes"
	grpcresolver "google.golang.org/grpc/resolver"
)

// scheme defines the protocol scheme name for this resolver
const scheme = "nacos"

// defaultPollInterval is the fallback refresh interval, a variable so tests can shorten it.
var defaultPollInterval = 5 * time.Second

// init automatically registers the resolver builder when the package is imported
func init() {
	grpcresolver.Register(&Builder{})
}

// Builder is the Builder structure for Nacos resolver
type Builder struct{}

// Build creates a new resolver instance based on target address
// Parameters:
//   - tgt: target address information
//   - cc: client connection interface
//   - opts: build options
//
// Returns:
//   - resolver.Resolver: resolver instance
//   - error: error information
func (b *Builder) Build(tgt grpcresolver.Target, cc grpcresolver.ClientConn, opts grpcresolver.BuildOptions) (grpcresolver.Resolver, error) {
	// Parse DSN configuration
	parsed, err := DefaultDsnParser(context.Background(), kindResolver, tgt.URL.String())
	if err != nil {
		return nil, err
	}

	// Create Nacos naming client
	client, err := newNamingClient(parsed.ClientParam)
	if err != nil {
		return nil, err
	}

	// Create resolver instance
	r := &resolver{
		target:       tgt,
		cc:           cc,
		namingClient: client,
		param:        parsed.SubscribeParam,
	}

	// Start the resolver
	if err := r.start(); err != nil {
		// Do not leak the client when the resolver never gets off the ground.
		client.CloseClient()
		return nil, err
	}

	return r, nil
}

// Scheme returns the protocol scheme supported by this resolver
func (b *Builder) Scheme() string {
	return scheme
}

// resolver is the concrete implementation of Nacos resolver
type resolver struct {
	target       grpcresolver.Target         // Target address information
	cc           grpcresolver.ClientConn     // Client connection interface
	namingClient naming_client.INamingClient // Nacos naming client
	param        vo.SubscribeParam           // Subscription parameters
	stopCh       chan struct{}               // Channel to stop polling goroutine
	resetCh      chan struct{}               // Channel to reset polling timer when subscription pushes
	closeOnce    sync.Once                   // Guard so Close can be called more than once
}

// start starts the resolver, performs initial service discovery,
// and starts both subscription and polling mechanisms
func (r *resolver) start() error {
	// Initialize channels
	r.stopCh = make(chan struct{})
	r.resetCh = make(chan struct{}, 1) // Buffered to avoid blocking

	// Set subscription callback function for real-time updates
	r.param.SubscribeCallback = func(services []model.Instance, err error) {
		if err != nil {
			return
		}
		r.updateState(services)
		// Notify polling goroutine to reset timer
		select {
		case r.resetCh <- struct{}{}:
		default:
		}
	}

	// Subscribe to service changes for real-time notifications
	if err := r.namingClient.Subscribe(&r.param); err != nil {
		return err
	}

	// Query initial service instance list
	if err := r.refreshInstances(); err != nil {
		// The subscription is live, take it back before giving up.
		_ = r.namingClient.Unsubscribe(&r.param)
		return err
	}

	// Start background polling goroutine as a fallback mechanism
	go r.poll()

	return nil
}

// refreshInstances queries and updates the service instance list
func (r *resolver) refreshInstances() error {
	instances, err := r.namingClient.SelectInstances(vo.SelectInstancesParam{
		ServiceName: r.param.ServiceName,
		GroupName:   r.param.GroupName,
		Clusters:    r.param.Clusters,
		HealthyOnly: true,
	})
	if err != nil {
		return err
	}

	// Update connection state
	r.updateState(instances)
	return nil
}

// poll periodically refreshes the service instance list as a fallback.
// When subscription pushes an update, the timer is reset to avoid immediate polling.
func (r *resolver) poll() {
	timer := time.NewTimer(defaultPollInterval)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			_ = r.refreshInstances()
			// Reset timer after polling
			timer.Reset(defaultPollInterval)
		case <-r.resetCh:
			// Subscription pushed an update, reset timer to delay next poll
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(defaultPollInterval)
		case <-r.stopCh:
			return
		}
	}
}

// updateState updates resolver state, converting service instances to gRPC address format
// Parameters:
//   - instances: Nacos service instance list
func (r *resolver) updateState(instances []model.Instance) {
	addrs := make([]grpcresolver.Address, 0, len(instances))

	// Iterate through all instances, filter healthy and enabled instances
	for _, inst := range instances {
		if !inst.Healthy || !inst.Enable {
			continue
		}

		// Construct gRPC address
		addr := grpcresolver.Address{
			Addr: net.JoinHostPort(inst.Ip, strconv.FormatUint(inst.Port, 10)),
		}

		// Add instance attribute information
		addr.Attributes = attributes.New("ServiceName", inst.ServiceName).
			WithValue("weight", inst.Weight).
			WithValue("cluster", inst.ClusterName).
			WithValue("ephemeral", inst.Ephemeral)

		// Add custom metadata
		for k, v := range inst.Metadata {
			addr.Attributes = addr.Attributes.WithValue(k, v)
		}

		addrs = append(addrs, addr)
	}

	// Sort addresses to ensure consistency
	slices.SortFunc(addrs, func(a, b grpcresolver.Address) int {
		return cmp.Compare(a.Addr, b.Addr)
	})

	// Update client connection state
	// Note: Even if addrs is empty, we still update the state.
	// This allows gRPC to handle the empty list appropriately (e.g., entering TRANSIENT_FAILURE).
	// When service instances become available later, the subscription callback will trigger
	// updateState again with non-empty addresses, and the connection will recover.
	//
	// A rejected update is not fatal: the polling fallback re-resolves on its own
	// schedule, so the error is dropped on purpose.
	_ = r.cc.UpdateState(grpcresolver.State{Addresses: addrs})
}

// ResolveNow resolves the target address immediately (currently implemented as no-op)
func (r *resolver) ResolveNow(grpcresolver.ResolveNowOptions) {}

// Close closes the resolver, stops polling, unsubscribes and closes the client.
// It is safe to call more than once.
func (r *resolver) Close() {
	r.closeOnce.Do(func() {
		// Stop the polling goroutine, if it was ever started.
		if r.stopCh != nil {
			close(r.stopCh)
		}
		_ = r.namingClient.Unsubscribe(&r.param)
		r.namingClient.CloseClient()
	})
}

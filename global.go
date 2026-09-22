package grpcnacos

import (
	"strings"
	"sync"
)

var registrarMu sync.RWMutex

var registrars = make(map[string]Factory)

// Register adds a Factory under name, ignoring the case of name. It panics
// when resource is nil or when name is already taken.
func Register(name string, resource Factory) {
	if resource == nil {
		panic("grpcnacos: Register resource is nil")
	}
	name = strings.ToLower(name)
	registrarMu.Lock()
	defer registrarMu.Unlock()
	if _, dup := registrars[name]; dup {
		panic("grpcnacos: Register called twice for resource " + name)
	}
	registrars[name] = resource
}

// Get returns the Factory registered under name, ignoring the case of name.
func Get(name string) (Factory, bool) {
	name = strings.ToLower(name)
	registrarMu.RLock()
	defer registrarMu.RUnlock()
	resource, ok := registrars[name]
	return resource, ok
}

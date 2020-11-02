package xclient

import (
	"errors"
	"github.com/tsundata/rpc/registry"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"
)

type SelectMode int

const (
	RandomSelect SelectMode = iota
	RoundRobinSelect
	WeightRoundRobinSelect
	WeightedICMP
	ConsistentHash
)

type Discovery interface {
	Refresh() error
	Update(servers []registry.ServerItem) error
	Get(servicePath string, mode SelectMode) (registry.ServerItem, error)
	GetAll(servicePath string) ([]registry.ServerItem, error)
}

type MultiServersDiscovery struct {
	r       *rand.Rand
	mu      sync.RWMutex
	servers []registry.ServerItem
	index   int
}

func NewMultiServerDiscovery(servers []registry.ServerItem) *MultiServersDiscovery {
	d := &MultiServersDiscovery{
		servers: servers,
		r:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	d.index = d.r.Intn(math.MaxInt32 - 1)
	return d
}

var _ Discovery = (*MultiServersDiscovery)(nil)

func (d *MultiServersDiscovery) Refresh() error {
	return nil
}

func (d *MultiServersDiscovery) Update(servers []registry.ServerItem) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers
	return nil
}

func (d *MultiServersDiscovery) Get(servicePath string, mode SelectMode) (registry.ServerItem, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.servers) == 0 {
		return registry.ServerItem{}, errors.New("rpc discovery: available servers")
	}

	// filter path
	var servers []registry.ServerItem
	for _, server := range d.servers {
		if strings.ToLower(server.ServicePath) == strings.ToLower(servicePath) {
			servers = append(servers, server)
		}
	}
	n := len(servers)
	if servers == nil || n == 0 {
		return registry.ServerItem{}, errors.New("rpc discovery: available servers")
	}

	switch mode {
	case RandomSelect:
		return servers[d.r.Intn(n)], nil
	case RoundRobinSelect:
		s := servers[d.index%n]
		d.index = (d.index + 1) % n
		return s, nil
	default:
		return registry.ServerItem{}, errors.New("rpc discovery: not supported select mode")
	}
}

func (d *MultiServersDiscovery) GetAll(servicePath string) ([]registry.ServerItem, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var servers []registry.ServerItem
	for _, server := range d.servers {
		if strings.ToLower(server.ServicePath) == strings.ToLower(servicePath) {
			servers = append(servers, server)
		}
	}

	return servers, nil
}

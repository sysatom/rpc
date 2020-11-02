package xclient

import (
	"errors"
	"github.com/tsundata/rpc/registry"
	"log"
	"net/http"
	"strings"
	"time"
)

type RegistryDiscovery struct {
	*MultiServersDiscovery
	registry   string
	timeout    time.Duration
	lastUpdate time.Time
}

const defaultUpdateTimeout = time.Second * 10

func NewRegistryDiscovery(registerAddr string, timeout time.Duration) *RegistryDiscovery {
	if timeout == 0 {
		timeout = defaultUpdateTimeout
	}
	d := &RegistryDiscovery{
		MultiServersDiscovery: NewMultiServerDiscovery(make([]registry.ServerItem, 0)),
		registry:              registerAddr,
		timeout:               timeout,
	}
	return d
}

func (d *RegistryDiscovery) Update(servers []registry.ServerItem) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers
	d.lastUpdate = time.Now()
	return nil
}

func (d *RegistryDiscovery) Refresh() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.lastUpdate.Add(d.timeout).After(time.Now()) {
		return nil
	}
	log.Println("rpc registry: refresh servers from registry", d.registry)
	resp, err := http.Get(d.registry)
	if err != nil {
		log.Println("rpc registry refresh err:", err)
		return err
	}
	servers := strings.Split(resp.Header.Get("X-RPC-Servers"), ",")
	d.servers = make([]registry.ServerItem, 0, len(servers))
	for _, server := range servers {
		if strings.TrimSpace(server) != "" {
			item := strings.Split(server, "|")
			if len(item) != 2 {
				return errors.New("ServerItem error")
			}
			d.servers = append(d.servers, registry.ServerItem{
				ServicePath: item[0],
				Addr:        item[1],
			})
		}
	}
	d.lastUpdate = time.Now()
	return nil
}

func (d *RegistryDiscovery) Get(servicePath string, mode SelectMode) (registry.ServerItem, error) {
	if err := d.Refresh(); err != nil {
		return registry.ServerItem{}, err
	}
	return d.MultiServersDiscovery.Get(servicePath, mode)
}

func (d *RegistryDiscovery) GetAll(servicePath string) ([]registry.ServerItem, error) {
	if err := d.Refresh(); err != nil {
		return nil, err
	}
	return d.MultiServersDiscovery.GetAll(servicePath)
}

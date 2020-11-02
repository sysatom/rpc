package xclient

import (
	"context"
	"github.com/tsundata/rpc/registry"
	"io"
	"reflect"
	"sync"

	. "github.com/tsundata/rpc"
)

type XClient struct {
	servicePath string
	d           Discovery
	mode        SelectMode
	opt         *Option
	mu          sync.Mutex
	clients     map[string]*Client
}

var _ io.Closer = (*XClient)(nil)

func NewXClient(servicePath string, d Discovery, mode SelectMode, opt *Option) *XClient {
	return &XClient{
		servicePath: servicePath,
		d:           d,
		mode:        mode,
		opt:         opt,
		clients:     make(map[string]*Client),
	}
}

func (xc *XClient) Close() error {
	xc.mu.Lock()
	defer xc.mu.Unlock()
	for key, client := range xc.clients {
		_ = client.Close()
		delete(xc.clients, key)
	}
	return nil
}

func (xc *XClient) dial(rpcAddr string) (*Client, error) {
	xc.mu.Lock()
	defer xc.mu.Unlock()
	client, ok := xc.clients[rpcAddr]
	if ok && !client.IsAvailable() {
		_ = client.Close()
		delete(xc.clients, rpcAddr)
		client = nil
	}
	if client == nil {
		var err error
		client, err = XDial(rpcAddr, xc.opt)
		if err != nil {
			return nil, err
		}
		xc.clients[rpcAddr] = client
	}
	return client, nil
}

func (xc *XClient) call(rpcAddr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
	client, err := xc.dial(rpcAddr)
	if err != nil {
		return err
	}
	return client.Call(ctx, xc.servicePath, serviceMethod, args, reply)
}

func (xc *XClient) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	serverItem, err := xc.d.Get(xc.servicePath, xc.mode)
	if err != nil {
		return err
	}
	return xc.call(serverItem.Addr, ctx, serviceMethod, args, reply)
}

func (xc *XClient) Broadcast(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	servers, err := xc.d.GetAll(xc.servicePath)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var e error
	replyDone := reply == nil
	ctx, cancel := context.WithCancel(ctx)
	for _, serverItem := range servers {
		wg.Add(1)
		go func(si registry.ServerItem) {
			defer wg.Done()
			var cloneReply interface{}
			if reply != nil {
				cloneReply = reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
			}
			err := xc.call(si.Addr, ctx, serviceMethod, args, cloneReply)
			mu.Lock()
			if err != nil && e == nil {
				e = err
				cancel()
			}
			if err == nil && !replyDone {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(cloneReply).Elem())
				replyDone = true
			}
			mu.Unlock()
		}(serverItem)
	}
	wg.Wait()
	return e
}

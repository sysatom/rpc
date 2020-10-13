package main

import (
	"fmt"
	"github.com/sysatom/rpc"
	"log"
	"net"
	"sync"
	"time"
)

func startServer(add chan string) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal("network error:", err)
	}
	log.Println("start rpc server on", l.Addr())
	add <- l.Addr().String()
	rpc.Accept(l)
}

func main() {
	log.SetFlags(0)
	addr := make(chan string)
	go startServer(addr)
	client, _ := rpc.Dial("tcp", <-addr)
	defer func() { _ = client.Close() }()

	time.Sleep(time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			args := fmt.Sprintf("rpc req %d", i)
			var reply string
			if err := client.Call("Foo.bar", args, &reply); err != nil {
				log.Fatal("call Foo.bar error:", err)
			}
			log.Println("reply", reply)
		}(i)
	}
	wg.Wait()
}

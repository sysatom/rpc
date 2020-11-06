package rpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/tsundata/rpc/codec"
)

const (
	AuthKey     = "__AUTH"
	MagicNumber = 0x3bef5c

	Connected        = "200 Connected to RPC"
	DefaultRPCPath   = "/_rpc_"
	DefaultDebugPath = "/debug/rpc"

	ReqMetaDataKey = "__req_metadata"
	ResMetaDataKey = "__res_metadata"
)

type Option struct {
	MagicNumber    int
	CodecType      codec.Type
	ConnectTimeout time.Duration
	HandleTimeout  time.Duration
}

var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	CodecType:      codec.GobType,
	ConnectTimeout: time.Second * 5,
}

type Server struct {
	serviceMap sync.Map

	// AuthFunc can be used to auth
	AuthFunc func(header *codec.Header, token string) error
}

func NewServer() *Server {
	return &Server{}
}

var DefaultServer = NewServer()

func (s *Server) Accept(lis net.Listener) {
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Println("rpc server: accept error:", err)
			return
		}
		go s.ServeConn(conn)
	}
}

func Accept(lis net.Listener) {
	DefaultServer.Accept(lis)
}

func (s *Server) ServeConn(conn io.ReadWriteCloser) {
	defer func() {
		_ = conn.Close()
	}()
	var opt Option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error", err)
		return
	}
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}
	s.serveCodec(f(conn), &opt)
}

var invalidRequest = struct{}{}

func (s *Server) serveCodec(cc codec.Codec, opt *Option) {
	sending := new(sync.Mutex)
	wg := new(sync.WaitGroup)
	for {
		req, err := s.readRequest(cc, opt.ConnectTimeout)
		if err != nil {
			if req == nil {
				break
			}
			req.h.Error = err.Error()
			s.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		go s.handleRequest(cc, req, sending, wg, opt.HandleTimeout)
	}
	wg.Wait()
	_ = cc.Close()
}

type request struct {
	h            *codec.Header
	argv, replyv reflect.Value
	mtype        *methodType
	svc          *service
}

func (s *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}

	return &h, nil
}

func (s *Server) readRequest(cc codec.Codec, timeout time.Duration) (*request, error) {
	read := make(chan struct{})
	errChan := make(chan error)
	reqChan := make(chan *request)
	go func() {
		h, err := s.readRequestHeader(cc)
		if err != nil {
			read<- struct{}{}
			errChan<- err
			reqChan<- nil
			return
		}
		req := &request{h: h}
		req.svc, req.mtype, err = s.findService(h.ServiceMethod)
		if err != nil {
			read<- struct{}{}
			errChan<- err
			reqChan<- nil
			return
		}
		req.argv = req.mtype.newArgv()
		req.replyv = req.mtype.newReplyv()

		argvi := req.argv.Interface()
		if req.argv.Type().Kind() != reflect.Ptr {
			argvi = req.argv.Addr().Interface()
		}
		if err = cc.ReadBody(argvi); err != nil {
			log.Println("rpc server: read body err:", err)
			read<- struct{}{}
			errChan<- err
			reqChan<- nil
			return
		}

		read<- struct{}{}
		reqChan<- req
		errChan<- nil
	}()
	
	if timeout == 0 {
		<-read
		return <-reqChan, <-errChan
	}

	select {
	case <-time.After(timeout):
		return nil, errors.New("read request timeout")
	case <-read:
		return <-reqChan, <-errChan
	}
}

func (s *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

func (s *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	// auth
	err := s.auth(req.h)
	if err != nil {
		req.h.Error = fmt.Sprintf("rpc server: auth error within %s", req.h.Metadata[AuthKey])
		s.sendResponse(cc, req.h, invalidRequest, sending)
		return
	}

	called := make(chan struct{})
	sent := make(chan struct{})
	go func() {
		err := req.svc.call(req.mtype, req.argv, req.replyv)
		called <- struct{}{}
		if err != nil {
			req.h.Error = err.Error()
			s.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		s.sendResponse(cc, req.h, req.replyv.Interface(), sending)
		sent <- struct{}{}
	}()

	if timeout == 0 {
		<-called
		<-sent
		return
	}
	select {
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle time expect within %s", timeout)
		s.sendResponse(cc, req.h, invalidRequest, sending)
	case <-called:
		<-sent
	}
}

func (s *Server) Register(rcvr interface{}) error {
	ss := newService(rcvr)
	if _, dup := s.serviceMap.LoadOrStore(ss.name, ss); dup {
		return errors.New("rpc: service already defined " + ss.name)
	}
	return nil
}

func Register(rcvr interface{}) error {
	return DefaultServer.Register(rcvr)
}

func (s *Server) findService(serviceMethod string) (svc *service, mtype *methodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	svci, ok := s.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service " + serviceName)
		return
	}
	svc = svci.(*service)
	mtype = svc.method[methodName]
	if mtype == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}

func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	_, _ = io.WriteString(conn, "HTTP/1.0 "+Connected+"\n\n")
	s.ServeConn(conn)
}

func (s *Server) HandleHTTP() {
	http.Handle(DefaultRPCPath, s)
	http.Handle(DefaultDebugPath, debugHTTP{s})
	log.Println("rpc server debug path:", DefaultDebugPath)
}

func (s *Server) auth(header *codec.Header) error {
	if s.AuthFunc != nil {
		token := header.Metadata[AuthKey]
		return s.AuthFunc(header, token)
	}

	return nil
}

func HandleHTTP() {
	DefaultServer.HandleHTTP()
}

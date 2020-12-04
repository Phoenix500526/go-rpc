package gorpc

import (
	"encoding/json"
	"fmt"
	"gorpc/codec"
	"io"
	"log"
	"net"
	"reflect"
	"sync"
)

const MagicNumber = 0x3bef5c

type Option struct {
	MagicNumber int
	CodecType   codec.Type
}

var DefaultOption = &Option{
	MagicNumber: MagicNumber,
	CodecType:   codec.GobType,
}

// RPC Server
type Server struct{}

// invalidRequest is a placeholder for response argv when error
var invalidRequest = struct{}{}

// request stores all information of a call
type request struct {
	header *codec.Header
	argv   reflect.Value
	replyv reflect.Value
}

func NewServer() *Server {
	return &Server{}
}

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

func (s *Server) ServeConn(conn io.ReadWriteCloser) {
	defer func() { _ = conn.Close() }()
	var opt Option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error: ", err)
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
	s.serveCodec(f(conn))
}

func (s *Server) serveCodec(c codec.Codec) {
	sending := new(sync.Mutex)
	wg := new(sync.WaitGroup)
	for {
		req, err := s.readRequest(c)
		if err != nil {
			if req == nil {
				break
			}
			req.header.Error = err.Error()
			s.sendResponse(c, req.header, invalidRequest, sending)
			continue
		}
		wg.Add(1)
		go s.handleRequest(c, req, sending, wg)
	}
	wg.Wait()
	_ = c.Close()
}

func (s *Server) readRequestHeader(c codec.Codec) (*codec.Header, error) {
	var header codec.Header
	if err := c.ReadHeader(&header); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error: ", err)
		}
		return nil, err
	}
	return &header, nil
}

func (s *Server) readRequest(c codec.Codec) (*request, error) {
	header, err := s.readRequestHeader(c)
	if err != nil {
		return nil, err
	}
	req := &request{header: header}
	req.argv = reflect.New(reflect.TypeOf(""))
	if err = c.ReadBody(req.argv.Interface()); err != nil {
		log.Println("rpc server: read argv err:", err)
	}
	return req, nil
}

func (s *Server) sendResponse(c codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := c.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

func (s *Server) handleRequest(c codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Println(req.header, req.argv.Elem())
	req.replyv = reflect.ValueOf(fmt.Sprintf("gorpc resp %d", req.header.Seq))
	s.sendResponse(c, req.header, req.replyv.Interface(), sending)
}

var DefaultServer = NewServer()

func Accept(lis net.Listener) { DefaultServer.Accept(lis) }

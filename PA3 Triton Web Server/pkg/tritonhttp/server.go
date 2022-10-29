package tritonhttp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"
)

type Server struct {
	// Addr specifies the TCP address for the server to listen on,
	// in the form "host:port". It shall be passed to net.Listen()
	// during ListenAndServe().
	Addr string // e.g. ":0"

	// DocRoot specifies the path to the directory to serve static files from.
	DocRoot string
}

// ListenAndServe listens on the TCP network address s.Addr and then
// handles requests on incoming connections.
func (s *Server) ListenAndServe() error {
	fmt.Println("hi server started")

	if err := s.ValidateServerSetup(); err != nil {
		return fmt.Errorf("server is not set up")
	}

	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("accept error, %v", err)
		}
		go s.HandleConnection(conn)
	}

	// Hint: call HandleConnection
}

// HandleConnection reads requests from the accepted conn and handles them.
func (s *Server) HandleConnection(conn net.Conn) {
	br := bufio.NewReader(conn)
	// Hint: use the other methods below
	fmt.Println("Server connected")
	for {
		// Set timeout
		if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			log.Printf("Failed to set timeout for connection %v", conn)
			_ = conn.Close()
			return
		}

		// Try to read next request
		req, isRead, err := ReadRequest(br)
		if isRead {
			fmt.Println("Byte is read")
		}

		// Handle EOF
		if errors.Is(err, io.EOF) {
			fmt.Println("EOF")
			_ = conn.Close()
			return
		}

		// Handle timeout
		if err, ok := err.(net.Error); ok && err.Timeout() && !isRead {
			fmt.Println("Timeout, conn closed")
			_ = conn.Close()
			return
		}

		// Handle bad request
		if req == nil {
			log.Printf("handle bad request for error: %v", err)
			res := &Response{}
			res.HandleBadRequest()
			_ = res.Write(conn)
			_ = conn.Close()
			return
		}

		// Handle good request
		res := s.HandleGoodRequest(req)
		err = res.Write(conn)
		if err != nil {
			fmt.Println(err)
		}

		// Close conn if requested/
		if req.Close {
			fmt.Println("system requested connection close")
			conn.Close()
			return
		}
	}

}

// HandleGoodRequest handles the valid req and generates the corresponding res.
func (s *Server) HandleGoodRequest(req *Request) (res *Response) {
	// Hint: use the other methods below
	fmt.Println("it seems good request 200")
	res = &Response{}
	res.Header = make(map[string]string)
	file, err := os.Stat(filepath.Join(s.DocRoot, req.URL))
	if os.IsNotExist(err) {
		res.HandleNotFound(req)
	} else {
		res.FilePath = filepath.Join(s.DocRoot, req.URL)
		res.Header["Content-Length"] = fmt.Sprintf("%d", file.Size())
		res.Header["Last-Modified"] = FormatTime(file.ModTime())
		res.HandleOK(req, res.FilePath)
	}
	return res
}

// HandleOK prepares res to be a 200 OK response
// ready to be written back to client.
func (res *Response) HandleOK(req *Request, path string) {
	res.Proto = "HTTP/1.1"
	res.StatusCode = 200
	res.Header["Date"] = FormatTime(time.Now())
	res.Header["Content-Type"] = MIMETypeByExtension(filepath.Ext(path))
	if req.Close {
		res.Header["Connection"] = "close"
	}
}

// HandleBadRequest prepares res to be a 400 Bad Request response
// ready to be written back to client.

func (res *Response) HandleBadRequest() {
	fmt.Println("it seems bad request 400")
	res.Header = make(map[string]string)
	res.Proto = "HTTP/1.1"
	res.StatusCode = 400
	res.FilePath = ""
	res.Header["Date"] = FormatTime(time.Now())
	res.Header["Connection"] = "close"
}

// HandleNotFound prepares res to be a 404 Not Found response
// ready to be written back to client.
func (res *Response) HandleNotFound(req *Request) {
	fmt.Println("it seems bad request 404")
	res.Header = make(map[string]string)
	res.Proto = "HTTP/1.1"
	res.StatusCode = 404
	res.FilePath = ""
	res.Header["Date"] = FormatTime(time.Now())
}

package tritonhttp

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Request struct {
	Method string // e.g. "GET"
	URL    string // e.g. "/path/to/a/file"
	Proto  string // e.g. "HTTP/1.1"

	// Header stores misc headers excluding "Host" and "Connection",
	// which are stored in special fields below.
	// Header keys are case-incensitive, and should be stored
	// in the canonical format in this map.
	Header map[string]string

	Host  string // determine from the "Host" header
	Close bool   // determine from the "Connection" header
}

// ReadRequest tries to read the next valid request from br.
//
// If it succeeds, it returns the valid request read. In this case,
// bytesReceived should be true, and err should be nil.
//
// If an error occurs during the reading, it returns the error,
// and a nil request. In this case, bytesReceived indicates whether or not
// some bytes are received before the error occurs. This is useful to determine
// the timeout with partial request received condition.
func ReadRequest(br *bufio.Reader) (req *Request, bytesReceived bool, err error) {
	req = &Request{}

	// Read start line
	req.Header = make(map[string]string)
	req.Proto = "HTTP/1.1"
	line, err := ReadLine(br)
	if err != nil {
		bytesReceived = false
		return nil, bytesReceived, err
	}

	req.Method, req.URL, err = parseRequestLine(line)
	req.URL = filepath.Clean(req.URL)
	if err != nil {
		bytesReceived = false
		return nil, bytesReceived, badStringError("malformed start line", line)
	}

	if !validMethod(req.Method) {
		bytesReceived = false
		return nil, bytesReceived, badStringError("invalid method", req.Method)
	}

	for {
		line, err := ReadLine(br)
		if err != nil {
			bytesReceived = false
			return nil, bytesReceived, err
		}

		if line == "" { // in case header end
			break
		}
		idx := strings.Index(line, ":")
		key := line[:idx]
		value := line[idx+1:]
		value = strings.TrimLeft(value, " ")
		if key == "Host" {
			req.Host = value
		} else if key == "Connection" {
			if strings.ToLower(value) == "close" {
				fmt.Println("connection is close")
				req.Close = true
			} else {
				fmt.Println("connection is not close")
				req.Close = false
			}
		} else {
			req.Header[key] = value
		}

	}

	// Check required host
	if req.Host == "" {
		bytesReceived = true
		return nil, bytesReceived, err
	}

	// Handle special headers

	fmt.Println("Request successfully sent")
	fmt.Println(req)
	return req, true, nil
}

func parseRequestLine(line string) (string, string, error) {
	fields := strings.SplitN(line, " ", 3)
	if len(fields) != 3 {
		return "", "", fmt.Errorf("could not parse the request line, got fields %v", fields)
	}
	return fields[0], fields[1], nil
}

func badStringError(what, val string) error {
	return fmt.Errorf("%s %q", what, val)
}

func (s *Server) ValidateServerSetup() error {
	fi, err := os.Stat(s.DocRoot)

	if os.IsNotExist(err) {
		return err
	}

	if !fi.IsDir() {
		return fmt.Errorf("doc root %q is not a directory", s.DocRoot)
	}
	return nil
}

func validMethod(method string) bool {
	return method == "GET"
}

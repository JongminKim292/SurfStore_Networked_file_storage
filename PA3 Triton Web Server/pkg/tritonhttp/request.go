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
	line, err := ReadLine(br)
	if err != nil {
		bytesReceived = false
		return nil, bytesReceived, err
	}

	req.Method, req.URL, req.Proto, err = parseRequestLine(line)
	newURL, urlIsValid := validURL(req.URL)
	req.URL = newURL
	if !urlIsValid {
		bytesReceived = false
		return nil, bytesReceived, badStringError("URL is not valid", req.URL)
	}

	if err != nil {
		bytesReceived = false
		return nil, bytesReceived, badStringError("malformed start line", line)
	}

	if !validMethod(req.Method) {
		bytesReceived = false
		return nil, bytesReceived, badStringError("invalid method", req.Method)
	}
	if !validProto(req.Proto) {
		bytesReceived = false
		return nil, bytesReceived, badStringError("invalid Proto", req.Proto)
	}
	bytesReceived = true

	for {
		line, err := ReadLine(br)
		if err != nil {
			fmt.Printf("%v err", err)
		}
		if line == "" { // in case header end
			fmt.Println("head reached endline break")
			break
		}

		idx := strings.Index(line, ":")
		if idx == -1 {
			return nil, bytesReceived, badStringError("invalid header", line)
		}
		key := CanonicalHeaderKey(line[:idx])
		value := line[idx+1:]
		value = strings.TrimLeft(value, " ")
		if key == "Host" {
			req.Host = value
		} else if key == "Connection" {
			if strings.ToLower(value) == "close" {
				req.Close = true
			} else {
				fmt.Println("invalid connection argument ~ bye")
				return nil, bytesReceived, badStringError("invalid close header", line)
			}
		} else {
			req.Header[key] = value
		}

	}

	// Check required host
	if req.Host == "" {
		bytesReceived = true
		fmt.Println("req.Host has error")
		return nil, bytesReceived, err
	}

	// Handle special headers

	fmt.Println("Request successfully sent")
	return req, true, nil
}

func parseRequestLine(line string) (string, string, string, error) {
	fields := strings.SplitN(line, " ", 3)
	if len(fields) != 3 {
		return "", "", "", fmt.Errorf("could not parse the request line, got fields %v", fields)
	}
	return fields[0], fields[1], fields[2], nil
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
func validProto(proto string) bool {
	return proto == "HTTP/1.1"
}
func validURL(URL string) (string, bool) {
	if len(URL) < 1 {
		return "", false
	}
	redirectedURL := URL
	firstCharacter := URL[0:1]
	lastCharacter := URL[len(URL)-1:]
	if firstCharacter != "/" {
		return "", false
	} else {
		if lastCharacter == "/" {
			redirectedURL = URL + "index.html"
		}
	}
	return filepath.Clean(redirectedURL), true
}

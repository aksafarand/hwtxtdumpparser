package structs

import (
	"bytes"
	"io"
)

type Table struct {
	Name      string
	Fpath     string
	Header    []string
	HeaderMap map[string]int64
	File      io.WriteCloser
	Buffer    *bytes.Buffer
}

package responses

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/tednaleid/ganda/config"
	"github.com/tednaleid/ganda/execcontext"
)

// syncBuffer is a goroutine-safe bytes.Buffer standing in for stdout
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

type failingWriter struct {
	err error
}

func (f *failingWriter) Write(p []byte) (int, error) {
	return 0, f.err
}

type failingReadCloser struct {
	err error
}

func (f *failingReadCloser) Read(p []byte) (int, error) {
	return 0, f.err
}

func (f *failingReadCloser) Close() error {
	return nil
}

func newRequestWithURL(fullURL string) *http.Request {
	parsed, _ := url.Parse(fullURL)
	return &http.Request{URL: parsed}
}

func newPrintingTestContext(t *testing.T, body config.ResponseBodyType, jsonEnvelope bool) *execcontext.Context {
	t.Helper()
	conf := config.New()
	conf.Silent = true
	context, err := execcontext.New(conf, strings.NewReader(""), io.Discard, &syncBuffer{})
	if err != nil {
		t.Fatal(err)
	}
	context.ResponseBody = body
	context.JsonEnvelope = jsonEnvelope
	return context
}

func parseEnvelope(t *testing.T, line string) map[string]interface{} {
	t.Helper()
	envelope := make(map[string]interface{})
	if err := json.Unmarshal([]byte(line), &envelope); err != nil {
		t.Fatalf("line was not a standalone parseable JSON object: %q (%v)", line, err)
	}
	return envelope
}

package cli

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// gatedServer holds every in-flight request until release() is called, so we
// can guarantee that many requests are being processed by different workers at
// the same instant and force response completion out of input order
type gatedServer struct {
	*HttpServerStub
	arrived  chan string
	release  chan struct{}
	seenMu   sync.Mutex
	seen     map[string]int
	released bool
}

func newGatedServer(t *testing.T) *gatedServer {
	t.Helper()
	gate := &gatedServer{
		arrived: make(chan string, 64),
		release: make(chan struct{}),
		seen:    make(map[string]int),
	}
	gate.HttpServerStub = NewHttpServerStub(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/item/")
		gate.seenMu.Lock()
		gate.seen[id]++
		gate.seenMu.Unlock()

		gate.arrived <- id
		<-gate.release // block until the test releases the whole batch

		// body is valid JSON and uniquely identifies the request so the
		// envelope's url/context/body can be cross-checked line by line
		fmt.Fprintf(w, `{"id":%q}`, id)
	}))
	return gate
}

func (g *gatedServer) waitForArrivals(t *testing.T, count int) {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for i := 0; i < count; i++ {
		select {
		case <-g.arrived:
		case <-timeout:
			t.Fatalf("only %d of %d requests arrived at the gated server", i, count)
		}
	}
}

func (g *gatedServer) openGate() {
	if !g.released {
		g.released = true
		close(g.release)
	}
}

// end-to-end regression: with many workers and JSON envelopes, every success
// must be a single standalone parseable JSON line whose url, context and body
// all belong to the same request, even when responses finish out of order and
// the same URL was submitted more than once
func TestConcurrentJsonEnvelopeOutputIsLinewiseParseable(t *testing.T) {
	const workers = 8
	const itemCount = 40

	server := newGatedServer(t)
	defer server.Server.Close()

	var inputLines []string
	idToContext := make(map[string]string)
	for i := 0; i < itemCount; i++ {
		id := fmt.Sprintf("item-%02d", i)
		// TSV: url plus a context column that should travel with this request
		contextValue := "ctx-" + id
		inputLines = append(inputLines, server.urlFor("item/"+id)+"\t"+contextValue)
		idToContext[id] = contextValue
	}
	// repeat a few URLs: duplicate input must not make contexts cross
	duplicates := []string{"item-03", "item-17", "item-17", "item-39"}
	for _, dup := range duplicates {
		inputLines = append(inputLines, server.urlFor("item/"+dup)+"\t"+"dup-"+dup)
	}
	totalLines := len(inputLines)

	runDone := make(chan GandaResults, 1)
	go func() {
		results, _ := RunGanda(
			[]string{"ganda", "-W", fmt.Sprint(workers), "-J", "-s"},
			strings.NewReader(strings.Join(inputLines, "\n")),
		)
		runDone <- results
	}()

	// wait until a full batch is concurrently in flight, then release them all
	server.waitForArrivals(t, workers)
	server.openGate()

	results := <-runDone

	// silent run: status logging stays on stderr but must be suppressed
	assert.Equal(t, "", results.stderr, "silent flag should keep stderr empty")

	reader := bufio.NewReader(strings.NewReader(results.stdout))
	parsed := 0
	seenContextPerID := make(map[string][]string)
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			if strings.TrimSpace(line) != "" {
				t.Fatalf("trailing output without newline: %q", line)
			}
			break
		}
		assert.NoError(t, err)

		var envelope struct {
			URL     string            `json:"url"`
			Code    int               `json:"code"`
			Body    map[string]string `json:"body"`
			Context []string          `json:"context"`
		}
		// every individual line must parse on its own, no spliced records
		if !assert.NoError(t, json.Unmarshal([]byte(strings.TrimRight(line, "\n")), &envelope), "bad line: %q", line) {
			continue
		}

		assert.Equal(t, http.StatusOK, envelope.Code)
		id := envelope.Body["id"]
		assert.Equal(t, server.urlFor("item/"+id), envelope.URL, "url does not match body")
		assert.Len(t, envelope.Context, 1, "context must stay attached to its own request")
		seenContextPerID[id] = append(seenContextPerID[id], envelope.Context[0])
		parsed++
	}

	assert.Equal(t, totalLines, parsed, "one JSON line per successful response")

	for id, contexts := range seenContextPerID {
		for _, ctxValue := range contexts {
			// each line's context must be one of the contexts submitted for
			// this exact url, never another request's
			assert.True(t,
				ctxValue == idToContext[id] || ctxValue == "dup-"+id,
				"context %q does not belong to %s", ctxValue, id)
		}
	}

	server.seenMu.Lock()
	defer server.seenMu.Unlock()
	assert.Equal(t, itemCount, len(server.seen), "every distinct URL was requested")
	assert.Equal(t, 3, server.seen["item-17"], "duplicated url was requested each time")
}

// non-JSON raw output must also stay unspliced under concurrency: each line is
// exactly one response body plus its trailing newline
func TestConcurrentRawOutputKeepsBodiesIntact(t *testing.T) {
	const workers = 6
	const itemCount = 30

	server := newGatedServer(t)
	defer server.Server.Close()

	var fragments []string
	for i := 0; i < itemCount; i++ {
		fragments = append(fragments, fmt.Sprintf("item/%02d", i))
	}

	runDone := make(chan GandaResults, 1)
	go func() {
		results, _ := RunGanda(
			[]string{"ganda", "-W", fmt.Sprint(workers), "-s"},
			server.stubStdinUrls(fragments),
		)
		runDone <- results
	}()

	server.waitForArrivals(t, workers)
	server.openGate()
	results := <-runDone

	assert.Equal(t, "", results.stderr)
	lines := strings.Split(strings.TrimRight(results.stdout, "\n"), "\n")
	assert.Len(t, lines, itemCount)

	expectedBodies := make(map[string]bool)
	for i := 0; i < itemCount; i++ {
		// paths are /item/NN so the stripped id is just the zero-padded number
		expectedBodies[fmt.Sprintf(`{"id":"%02d"}`, i)] = true
	}
	got := make(map[string]int)
	for _, line := range lines {
		assert.True(t, expectedBodies[line], "unexpected or spliced body line: %q", line)
		got[line]++
	}
	assert.Len(t, got, itemCount)
}

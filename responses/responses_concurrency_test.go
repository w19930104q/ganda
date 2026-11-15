package responses

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tednaleid/ganda/config"
	"github.com/tednaleid/ganda/execcontext"
)

// every response must show up as exactly one intact line, no matter how the
// workers interleave; this is the regression test for stdout records from
// multiple workers being spliced together
func TestStartResponseWorkersConcurrentEnvelopesAreAtomicLines(t *testing.T) {
	t.Parallel()

	const workers = 6
	const perWorker = 25

	context := newPrintingTestContext(t, config.Raw, true)
	context.ResponseWorkers = workers
	context.RequestWorkers = workers

	responsesChan := make(chan *ResponseWithContext, workers)
	waitGroup, controller := StartResponseWorkers(responsesChan, context)

	expected := make(map[string]bool)
	var expectedMu sync.Mutex

	var feedWg sync.WaitGroup
	for w := 0; w < workers; w++ {
		feedWg.Add(1)
		go func(worker int) {
			defer feedWg.Done()
			for i := 0; i < perWorker; i++ {
				id := strings.Repeat("x", worker) + "-" + strings.Repeat("y", i)
				url := "http://example.com/resource/" + id
				// body is itself JSON so it can be emitted raw inside the envelope
				body := `{"id":"` + id + `"}`
				mock := NewMockResponse(body, url, 200)
				expectedMu.Lock()
				expected[id] = false
				expectedMu.Unlock()
				responsesChan <- &ResponseWithContext{
					Response:       mock.Response,
					RequestContext: []string{"ctx-" + id},
				}
			}
		}(w)
	}
	feedWg.Wait()
	close(responsesChan)
	waitGroup.Wait()
	assert.NoError(t, controller.Wait())

	stdout := context.Out.(*syncBuffer).String()
	lines := strings.Split(strings.TrimRight(stdout, "\n"), "\n")
	assert.Len(t, lines, workers*perWorker, "one JSON line per successful response")

	seen := make(map[string]bool)
	for _, line := range lines {
		envelope := parseEnvelope(t, line)

		assert.Contains(t, envelope, "url")
		assert.Contains(t, envelope, "code")
		assert.Contains(t, envelope, "body")
		assert.Contains(t, envelope, "context")

		body := envelope["body"].(map[string]interface{})
		id := body["id"].(string)

		// url, body and context must all describe the same request
		assert.Equal(t, "http://example.com/resource/"+id, envelope["url"])
		assert.Equal(t, []interface{}{"ctx-" + id}, envelope["context"])
		assert.Equal(t, float64(200), envelope["code"])

		assert.False(t, seen[id], "duplicate line for request %s", id)
		seen[id] = true
	}

	assert.Len(t, seen, len(expected))
	for id := range expected {
		assert.True(t, seen[id], "missing response for %s", id)
	}
}

// duplicate input URLs must each keep their own context, workers must never
// attach one request's context/body to another request's envelope
func TestStartResponseWorkersDuplicateUrlsKeepDistinctContext(t *testing.T) {
	t.Parallel()

	context := newPrintingTestContext(t, config.Raw, true)
	context.ResponseWorkers = 4
	context.RequestWorkers = 4

	responsesChan := make(chan *ResponseWithContext, 4)
	waitGroup, controller := StartResponseWorkers(responsesChan, context)

	const duplicates = 30
	url := "http://example.com/same"
	go func() {
		for i := 0; i < duplicates; i++ {
			mock := NewMockResponse(`{"n":0}`, url, 200)
			responsesChan <- &ResponseWithContext{
				Response:       mock.Response,
				RequestContext: i,
			}
		}
		close(responsesChan)
	}()

	waitGroup.Wait()
	assert.NoError(t, controller.Wait())

	lines := strings.Split(strings.TrimRight(context.Out.(*syncBuffer).String(), "\n"), "\n")
	assert.Len(t, lines, duplicates)

	seenContexts := make(map[int]bool)
	for _, line := range lines {
		envelope := parseEnvelope(t, line)
		assert.Equal(t, url, envelope["url"])
		assert.Equal(t, map[string]interface{}{"n": float64(0)}, envelope["body"])
		n := int(envelope["context"].(float64))
		assert.False(t, seenContexts[n], "context %d was attached to two responses", n)
		seenContexts[n] = true
	}
	assert.Len(t, seenContexts, duplicates)
}

// a failure while reading a response body is fatal: the error is reported and
// no further responses are published
func TestStartResponseWorkersBodyReadErrorStopsPublishing(t *testing.T) {
	t.Parallel()

	context := newPrintingTestContext(t, config.Raw, true)
	context.ResponseWorkers = 3
	context.RequestWorkers = 3

	responsesChan := make(chan *ResponseWithContext, 8)
	waitGroup, controller := StartResponseWorkers(responsesChan, context)

	boom := errors.New("simulated body read failure")
	for i := 0; i < 6; i++ {
		response := &http.Response{
			StatusCode: 200,
			Body:       &failingReadCloser{err: boom},
			Request:    newRequestWithURL("http://example.com/boom/" + strconv.Itoa(i)),
		}
		responsesChan <- &ResponseWithContext{Response: response, RequestContext: i}
	}
	close(responsesChan)
	waitGroup.Wait()

	err := controller.Wait()
	assert.Error(t, err)
	assert.ErrorIs(t, err, boom)

	// nothing publishable should have made it to stdout
	assert.Equal(t, "", context.Out.(*syncBuffer).String())
}

// a failure while writing to stdout is fatal: publishing stops and the error
// is surfaced, no partial record is allowed through
func TestStartResponseWorkersStdoutWriteErrorStopsPublishing(t *testing.T) {
	t.Parallel()

	conf := config.New()
	conf.Silent = true
	context, err := execcontext.New(conf, strings.NewReader(""), io.Discard, &failingWriter{err: errors.New("stdout is broken")})
	assert.NoError(t, err)
	context.JsonEnvelope = true
	context.ResponseBody = config.Raw
	context.ResponseWorkers = 4
	context.RequestWorkers = 4

	responsesChan := make(chan *ResponseWithContext, 8)
	waitGroup, controller := StartResponseWorkers(responsesChan, context)

	for i := 0; i < 8; i++ {
		mock := NewMockResponse(`{"ok":true}`, "http://example.com/x/"+string(rune('a'+i)), 200)
		responsesChan <- &ResponseWithContext{Response: mock.Response, RequestContext: i}
	}
	close(responsesChan)
	waitGroup.Wait()

	err = controller.Wait()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "stdout is broken")

	// the run's context must have been cancelled so upstream request work stops
	select {
	case <-context.Ctx.Done():
	default:
		t.Fatal("expected run context to be cancelled after a fatal publish error")
	}
}

package responses

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/tednaleid/ganda/config"
	"github.com/tednaleid/ganda/execcontext"
	"hash"
	"io"
	"net/http"
	"os"
	"regexp"
	"sync"
)

var specialCharactersRegexp = regexp.MustCompile("[^A-Za-z0-9]+")

type ResponseWithContext struct {
	Response       *http.Response
	RequestContext interface{}
}

// publishRecord is one fully buffered output line plus the response metadata
// needed for status logging; it is handed to the single stdout publisher so a
// record can never be interleaved with another worker's writes
type publishRecord struct {
	record       []byte
	response     *http.Response
	bytesWritten int64
}

// PublishController lets the orchestrator wait for all stdout publishing to
// drain and inspect the first fatal error (body read or stdout write failure)
type PublishController struct {
	publisherWaitGroup *sync.WaitGroup
	fatal              chan error
	once               sync.Once
}

// Wait blocks until every buffered record has been published (or publishing
// stopped on a fatal error) and returns the first fatal error, if any
func (controller *PublishController) Wait() error {
	controller.publisherWaitGroup.Wait()
	select {
	case err := <-controller.fatal:
		return err
	default:
		return nil
	}
}

// reportFatal records the first failure once, logs it explicitly and cancels
// the run so in-flight request workers abort and no further records publish
func (controller *PublishController) reportFatal(context *execcontext.Context, err error) {
	controller.once.Do(func() {
		controller.fatal <- err
		close(controller.fatal)
		context.Logger.LogError(err, "fatal response processing error")
		context.Cancel()
	})
}

func StartResponseWorkers(responsesWithContext <-chan *ResponseWithContext, context *execcontext.Context) (*sync.WaitGroup, *PublishController) {
	// file saving workers each own their own file, they never share stdout,
	// so they keep the simple fan-out without a publisher
	if context.WriteFiles {
		return startFileSavingWorkers(responsesWithContext, context), nil
	}
	return startPrintingWorkers(responsesWithContext, context)
}

// startPrintingWorkers fans response processing out across ResponseWorkers
// goroutines; each one buffers a complete record before sending it to a single
// publisher goroutine that is the only writer touching stdout, which makes
// every emitted line atomic regardless of completion order
func startPrintingWorkers(
	responsesWithContext <-chan *ResponseWithContext,
	context *execcontext.Context,
) (*sync.WaitGroup, *PublishController) {
	var responseWaitGroup sync.WaitGroup
	responseWaitGroup.Add(context.ResponseWorkers)

	records := make(chan *publishRecord, context.ResponseWorkers)

	controller := &PublishController{
		publisherWaitGroup: &sync.WaitGroup{},
		fatal:              make(chan error, 1),
	}
	controller.publisherWaitGroup.Add(1)
	go recordPublisher(records, context, controller)

	newline := []byte("\n")

	for i := 1; i <= context.ResponseWorkers; i++ {
		go func() {
			defer responseWaitGroup.Done()

			var emitResponse emitResponseWithContextFn
			if context.JsonEnvelope {
				emitResponse = determineEmitJsonResponseWithContextFn(context.ResponseBody)
			} else {
				emitResponse = determineEmitResponseFn(context.ResponseBody)
			}

			for responseWithContext := range responsesWithContext {
				if isShuttingDown(context) {
					closeResponseBody(responseWithContext)
					continue
				}

				record := new(bytes.Buffer)
				bytesWritten, err := emitResponse(responseWithContext, record)

				if err != nil {
					controller.reportFatal(context, fmt.Errorf(
						"failed to read response body from %s: %w",
						responseWithContext.Response.Request.URL.String(), err))
					continue
				}

				// preserve the original behavior of one trailing newline per
				// non-empty response, included in the atomic record
				if bytesWritten > 0 {
					record.Write(newline)
				}

				// the publisher may have stopped after a fatal write error;
				// bail out instead of blocking on a channel nobody reads
				select {
				case records <- &publishRecord{
					record:       record.Bytes(),
					response:     responseWithContext.Response,
					bytesWritten: bytesWritten,
				}:
				case <-context.Ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		responseWaitGroup.Wait()
		close(records)
	}()

	return &responseWaitGroup, controller
}

// recordPublisher is the sole writer to stdout; records may arrive in any
// order but each write is one complete line, so downstream line parsers never
// see spliced JSON. A write failure is fatal: the run context is cancelled so
// request workers stop fetching, and no further records are published.
func recordPublisher(records <-chan *publishRecord, context *execcontext.Context, controller *PublishController) {
	defer controller.publisherWaitGroup.Done()

	for record := range records {
		if isShuttingDown(context) {
			continue
		}

		if _, err := context.Out.Write(record.record); err != nil {
			controller.reportFatal(context, fmt.Errorf(
				"failed to write response for %s to stdout: %w",
				record.response.Request.URL.String(), err))
			return
		}

		context.Logger.LogResponse(record.response.StatusCode, record.response.Request.URL.String())
	}
}

func isShuttingDown(context *execcontext.Context) bool {
	select {
	case <-context.Ctx.Done():
		return true
	default:
		return false
	}
}

func closeResponseBody(responseWithContext *ResponseWithContext) {
	if responseWithContext != nil && responseWithContext.Response != nil && responseWithContext.Response.Body != nil {
		responseWithContext.Response.Body.Close()
	}
}

// creates a worker that takes responses off the channel and saves each one to a file
// the directory path is based off the md5 hash of the url
// the filename is the url with all non-alphanumeric characters replaced with dashes
func startFileSavingWorkers(
	responsesWithContext <-chan *ResponseWithContext,
	context *execcontext.Context,
) *sync.WaitGroup {
	var responseWaitGroup sync.WaitGroup
	responseWaitGroup.Add(context.ResponseWorkers)

	for i := 1; i <= context.ResponseWorkers; i++ {
		go func() {
			var emitResponse emitResponseWithContextFn
			if context.JsonEnvelope {
				emitResponse = determineEmitJsonResponseWithContextFn(context.ResponseBody)
			} else {
				emitResponse = determineEmitResponseFn(context.ResponseBody)
			}
			responseSavingWorker(responsesWithContext, context, emitResponse)
			responseWaitGroup.Done()
		}()
	}

	return &responseWaitGroup
}

func responseSavingWorker(
	responsesWithContext <-chan *ResponseWithContext,
	context *execcontext.Context,
	emitResponseWithContextFn emitResponseWithContextFn,
) {
	responseWorker(responsesWithContext, func(responseWithContext *ResponseWithContext) {
		response := responseWithContext.Response
		filename := specialCharactersRegexp.ReplaceAllString(response.Request.URL.String(), "-")
		writeableFile, err := createWritableFile(context.BaseDirectory, context.SubdirLength, filename)
		if err != nil {
			context.Logger.LogError(err, response.Request.URL.String())
			return
		}
		defer writeableFile.WriteCloser.Close()

		_, err = emitResponseWithContextFn(responseWithContext, writeableFile.WriteCloser)

		if err != nil {
			context.Logger.LogError(err, response.Request.URL.String()+" -> "+writeableFile.FullPath)
		} else {
			context.Logger.LogResponse(response.StatusCode, response.Request.URL.String()+" -> "+writeableFile.FullPath)
		}
	})
}

// takes a response and writes it to the writer, returns true if it wrote anything
type emitResponseFn func(response *http.Response, out io.Writer) (bytesWritten int64, err error)
type emitResponseWithContextFn func(responseWithContext *ResponseWithContext, out io.Writer) (bytesWritten int64, err error)

// emits the response without the context, context is only supported in JSON output
func determineEmitResponseFn(responseBody config.ResponseBodyType) emitResponseWithContextFn {
	bodyResponseFn := determineEmitBodyResponseFn(responseBody)

	return func(responseWithContext *ResponseWithContext, out io.Writer) (bytesWritten int64, err error) {
		return bodyResponseFn(responseWithContext.Response, out)
	}
}

// surrounds the responsesBody with a JSON envelope that includes the context of the request (if any)
func determineEmitJsonResponseWithContextFn(responseBody config.ResponseBodyType) emitResponseWithContextFn {
	bodyResponseFn := determineEmitBodyResponseFn(responseBody)
	return jsonEnvelopeResponseFn(bodyResponseFn, responseBody)
}

// jsonEnvelope is buffered as a whole so every emitted record is guaranteed
// to be a single valid JSON object: url, status code, body and the original
// request context always describe the same request.
//
// rawBody holds already-JSON content (Raw and Escaped body modes) that is
// embedded verbatim instead of being re-quoted; stringBody holds the quoted
// scalar values produced by base64/sha256 modes.
type jsonEnvelope struct {
	url          string
	code         int
	rawBody      []byte
	stringBody   string
	bodyPresent  bool
	requestCtx   interface{}
	contextGiven bool
}

// isInlineJSON reports whether body is valid JSON that can be embedded
// directly into a single-line envelope: it must parse as JSON and contain no
// raw newline, otherwise the record would span multiple stdout lines
func isInlineJSON(body []byte) bool {
	return bytes.IndexByte(body, '\n') < 0 && bytes.IndexByte(body, '\r') < 0 && json.Valid(body)
}

// writeTo serializes the envelope in the original spaced format
// (`{ "url": ..., "code": ..., "body": ... }`) while letting encoding/json
// handle all escaping for arbitrary URLs and contexts
func (envelope jsonEnvelope) writeTo(buffer *bytes.Buffer) error {
	urlJSON, err := json.Marshal(envelope.url)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(buffer, "{ \"url\": %s, \"code\": %d, \"body\": ", urlJSON, envelope.code); err != nil {
		return err
	}

	switch {
	case !envelope.bodyPresent:
		buffer.WriteString("null")
	case isInlineJSON(envelope.rawBody):
		// Raw/Escaped content that is already single-line valid JSON is
		// embedded verbatim
		buffer.Write(envelope.rawBody)
	case envelope.rawBody != nil:
		// not standalone single-line JSON (e.g. contains a raw newline):
		// quote and escape it as a string so the record stays one parseable
		// JSON line and the original body bytes are preserved
		escaped, err := json.Marshal(string(envelope.rawBody))
		if err != nil {
			return err
		}
		buffer.Write(escaped)
	default:
		stringJSON, err := json.Marshal(envelope.stringBody)
		if err != nil {
			return err
		}
		buffer.Write(stringJSON)
	}

	if envelope.contextGiven && envelope.requestCtx != nil {
		contextJSON, err := json.Marshal(envelope.requestCtx)
		if err != nil {
			return err
		}
		if string(contextJSON) != "null" {
			if _, err := fmt.Fprintf(buffer, ", \"context\": %s", string(contextJSON)); err != nil {
				return err
			}
		}
	}

	buffer.WriteString(" }")
	return nil
}

// returns a function that buffers the complete JSON envelope (url, code, body
// and request context) into the writer as one write, so concurrent workers
// can never splice fields from different requests into the same line
func jsonEnvelopeResponseFn(bodyResponseFn emitResponseFn, responseBody config.ResponseBodyType) emitResponseWithContextFn {
	return func(responseWithContext *ResponseWithContext, out io.Writer) (bytesWritten int64, err error) {
		response := responseWithContext.Response

		envelope := jsonEnvelope{
			url:          response.Request.URL.String(),
			code:         response.StatusCode,
			requestCtx:   responseWithContext.RequestContext,
			contextGiven: responseWithContext.RequestContext != nil,
		}

		// Raw and Escaped bodies are assumed to already be valid JSON (or
		// empty); base64/sha256 produce a plain quoted scalar; discard emits
		// nothing. Buffer the body so the whole envelope stays one write.
		switch responseBody {
		case config.Raw, config.Escaped:
			bodyBuffer := new(bytes.Buffer)
			bodyBytesWritten, bodyErr := bodyResponseFn(response, bodyBuffer)
			if bodyErr != nil {
				return 0, bodyErr
			}
			if bodyBytesWritten > 0 {
				envelope.rawBody = bodyBuffer.Bytes()
				envelope.bodyPresent = true
				bytesWritten = bodyBytesWritten
			}
		case config.Discard:
			// still owns and must close the response body, nothing to emit
			if _, bodyErr := bodyResponseFn(response, io.Discard); bodyErr != nil {
				return 0, bodyErr
			}
		default:
			// base64/sha256 produce a plain string value that gets quoted
			bodyBuffer := new(bytes.Buffer)
			bodyBytesWritten, bodyErr := bodyResponseFn(response, bodyBuffer)
			if bodyErr != nil {
				return 0, bodyErr
			}
			if bodyBytesWritten > 0 {
				envelope.stringBody = bodyBuffer.String()
				envelope.bodyPresent = true
				bytesWritten = bodyBytesWritten
			}
		}

		record := new(bytes.Buffer)
		if err := envelope.writeTo(record); err != nil {
			return 0, err
		}

		written, err := out.Write(record.Bytes())
		return int64(written) + bytesWritten, err
	}
}

func determineEmitBodyResponseFn(responseBody config.ResponseBodyType) emitResponseFn {
	switch responseBody {
	case config.Raw:
		return emitRawBody
	case config.Sha256:
		return emitSha256BodyFn() //nolint:bodyclose // body is closed inside the returned closure
	case config.Discard:
		return emitNothingBody
	case config.Escaped:
		return emitEscapedBodyFn() //nolint:bodyclose // body is closed inside the returned closure
	case config.Base64:
		return emitBase64Body
	default:
		panic(fmt.Sprintf("unknown response body type %s", responseBody))
	}
}

func emitRawBody(response *http.Response, out io.Writer) (bytesWritten int64, err error) {
	defer response.Body.Close()
	return io.Copy(out, response.Body)
}

func emitSha256BodyFn() func(response *http.Response, out io.Writer) (bytesWritten int64, err error) {
	hasher := sha256.New()
	return func(response *http.Response, out io.Writer) (bytesWritten int64, err error) {
		return emitHashedBody(hasher, response, out)
	}
}

func emitHashedBody(hasher hash.Hash, response *http.Response, out io.Writer) (bytesWritten int64, err error) {
	defer response.Body.Close()

	hasher.Reset()

	hashedBytesWritten, err := io.Copy(hasher, response.Body)
	if err != nil || hashedBytesWritten == 0 {
		return 0, err
	}

	n, err := fmt.Fprint(out, hex.EncodeToString(hasher.Sum(nil)))
	return int64(n), err
}

func emitBase64Body(response *http.Response, out io.Writer) (bytesWritten int64, err error) {
	defer response.Body.Close()

	encoder := base64.NewEncoder(base64.StdEncoding, out)
	bytesWritten, err = io.Copy(encoder, response.Body)
	if err != nil {
		return bytesWritten, err
	}

	err = encoder.Close()
	return bytesWritten, err
}

func emitEscapedBodyFn() func(response *http.Response, out io.Writer) (bytesWritten int64, err error) {
	buffer := new(bytes.Buffer)
	return func(response *http.Response, out io.Writer) (bytesWritten int64, err error) {
		return emitEscapedBody(buffer, response, out)
	}
}

func emitEscapedBody(buffer *bytes.Buffer, response *http.Response, out io.Writer) (bytesWritten int64, err error) {
	defer response.Body.Close()

	buffer.Reset()

	_, err = io.Copy(buffer, response.Body)
	if err != nil {
		return 0, err
	}

	if buffer.Len() == 0 {
		return 0, nil
	}

	// Marshal the buffer's contents to JSON
	jsonBytes, err := json.Marshal(buffer.String())
	if err != nil {
		return 0, err
	}

	// Write the JSON bytes to the output writer
	n, err := out.Write(jsonBytes)
	return int64(n), err
}

func emitNothingBody(response *http.Response, out io.Writer) (bytesWritten int64, err error) {
	response.Body.Close()
	return 0, nil
}

func responseWorker(responsesWithContext <-chan *ResponseWithContext, responseHandler func(*ResponseWithContext)) {
	for responseWithContext := range responsesWithContext {
		responseHandler(responseWithContext)
	}
}

type WritableFile struct {
	FullPath    string
	WriteCloser io.WriteCloser
}

func createWritableFile(baseDirectory string, subdirLength int, filename string) (*WritableFile, error) {
	directory := directoryForFile(baseDirectory, filename, subdirLength)
	fullPath := directory + filename

	file, err := os.Create(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file %s: %w", fullPath, err)
	}

	return &WritableFile{FullPath: fullPath, WriteCloser: file}, nil
}

func directoryForFile(baseDirectory string, filename string, subdirLength int) string {
	var directory string
	if subdirLength <= 0 {
		directory = fmt.Sprintf("%s/", baseDirectory)
	} else {
		sliceEnd := 1

		// don't create directories longer than 4 binary hex characters (4^16 = 65k directories)
		if subdirLength > 2 {
			sliceEnd = 2
		}

		md5val := md5.Sum([]byte(filename))
		directory = fmt.Sprintf("%s/%x/", baseDirectory, md5val[0:sliceEnd])
	}

	os.MkdirAll(directory, os.ModePerm)
	return directory
}

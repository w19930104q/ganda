package requests

import (
	"crypto/tls"
	"fmt"
	"github.com/tednaleid/ganda/execcontext"
	"github.com/tednaleid/ganda/logger"
	"github.com/tednaleid/ganda/parser"
	"github.com/tednaleid/ganda/responses"
	"net/http"
	"sync"
	"time"
)

type HttpClient struct {
	MaxRetries int
	Client     *http.Client
	Logger     *logger.LeveledLogger
}

func NewHttpClient(context *execcontext.Context) *HttpClient {
	return &HttpClient{
		MaxRetries: context.Retries,
		Logger:     context.Logger,
		Client: &http.Client{
			Timeout: context.ConnectTimeoutDuration,
			Transport: &http.Transport{
				MaxIdleConns:        500,
				MaxIdleConnsPerHost: 50,
				MaxConnsPerHost:     50,
				IdleConnTimeout:     90 * time.Second,
				ForceAttemptHTTP2:   true,
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: context.Insecure,
				},
			},
		},
	}
}

func StartRequestWorkers(
	requestsWithContext <-chan parser.RequestWithContext,
	responsesWithContext chan<- *responses.ResponseWithContext,
	rateLimitTicker *time.Ticker,
	context *execcontext.Context,
) *sync.WaitGroup {
	var requestWaitGroup sync.WaitGroup
	requestWaitGroup.Add(context.RequestWorkers)

	for i := 1; i <= context.RequestWorkers; i++ {
		go func() {
			requestWorker(context, requestsWithContext, responsesWithContext, rateLimitTicker)
			requestWaitGroup.Done()
		}()
	}

	return &requestWaitGroup
}

func requestWorker(
	context *execcontext.Context,
	requestsWithContext <-chan parser.RequestWithContext,
	responsesWithContext chan<- *responses.ResponseWithContext,
	rateLimitTicker *time.Ticker,
) {
	httpClient := NewHttpClient(context)

	for requestWithContext := range requestsWithContext {
		// a fatal response-processing error cancels the run; stop pulling
		// new work and close out any requests still in the channel
		select {
		case <-context.Ctx.Done():
			continue
		default:
		}

		if rateLimitTicker != nil {
			select {
			case <-rateLimitTicker.C: // wait for the next tick to send the request
			case <-context.Ctx.Done():
				continue
			}
		}

		// bind the run context so an in-flight request is aborted when the
		// run shuts down instead of lingering until its own timeout
		requestWithContext.Request = requestWithContext.Request.WithContext(context.Ctx)

		finalResponse, err := requestWithRetry(httpClient, requestWithContext, context.BaseRetryDelayDuration)

		if err != nil {
			httpClient.Logger.LogError(err, requestWithContext.Request.URL.String())
		} else {
			select {
			case responsesWithContext <- finalResponse:
			case <-context.Ctx.Done():
				// nobody is going to consume this response anymore
				if finalResponse != nil && finalResponse.Response != nil {
					finalResponse.Response.Body.Close()
				}
				return
			}
		}
	}
}

func requestWithRetry(
	httpClient *HttpClient,
	requestWithContext parser.RequestWithContext,
	baseRetryDelay time.Duration,
) (*responses.ResponseWithContext, error) {
	var response *http.Response
	var err error

	for attempts := 1; ; attempts++ {
		response, err = httpClient.Client.Do(requestWithContext.Request)

		responseWithContext := &responses.ResponseWithContext{
			Response:       response,
			RequestContext: requestWithContext.RequestContext,
		}

		if err == nil && response.StatusCode < 500 {
			// return successful response or non-server error, we don't retry those
			return responseWithContext, nil
		}

		message := requestWithContext.Request.URL.String()

		if err == nil {
			httpClient.Logger.LogResponse(response.StatusCode, message)
			response.Body.Close()
		} else {
			httpClient.Logger.LogError(err, message)
		}

		if attempts > httpClient.MaxRetries {
			return responseWithContext, fmt.Errorf("maximum number of retries (%d) reached for request", httpClient.MaxRetries)
		}

		delay := baseRetryDelay * time.Duration(1<<attempts)
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}
		select {
		case <-time.After(delay):
		case <-requestWithContext.Request.Context().Done():
			return nil, requestWithContext.Request.Context().Err()
		}
	}

}

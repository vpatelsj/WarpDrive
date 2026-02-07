package auth

import (
	"fmt"
	"net/http"
	"time"
)

// retryableStatusCodes are HTTP status codes that trigger a retry.
var retryableStatusCodes = map[int]bool{
	http.StatusTooManyRequests:     true, // 429
	http.StatusInternalServerError: true, // 500
	http.StatusServiceUnavailable:  true, // 503
}

// retryDo executes an HTTP request with retry logic.
// Retries up to 3 attempts with exponential backoff (1s → 2s → 4s)
// on HTTP 429, 500, and 503 responses.
func retryDo(client *http.Client, req *http.Request) (*http.Response, error) {
	return retryDoWithBackoffs(client, req, defaultBackoffs)
}

// defaultBackoffs is the production backoff schedule.
var defaultBackoffs = []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second}

// retryDoWithBackoffs executes an HTTP request with configurable retry backoffs.
func retryDoWithBackoffs(client *http.Client, req *http.Request, backoffs []time.Duration) (*http.Response, error) {
	maxAttempts := len(backoffs)

	var lastErr error
	var lastResp *http.Response

	for attempt := 0; attempt < maxAttempts; attempt++ {
		// Clone the request for retries (body may have been consumed)
		var reqClone *http.Request
		if attempt == 0 {
			reqClone = req
		} else {
			// For retries, we need a fresh request. If the original body
			// was already consumed, we need GetBody.
			if req.GetBody != nil {
				body, err := req.GetBody()
				if err != nil {
					return nil, fmt.Errorf("retry: get body: %w", err)
				}
				reqClone = req.Clone(req.Context())
				reqClone.Body = body
			} else if req.Body == nil {
				reqClone = req.Clone(req.Context())
			} else {
				// If there's a body but no GetBody, we can't retry safely
				// Return the last error/response
				if lastResp != nil {
					return lastResp, nil
				}
				return nil, lastErr
			}
		}

		resp, err := client.Do(reqClone)
		if err != nil {
			lastErr = err
			if attempt < maxAttempts-1 {
				select {
				case <-req.Context().Done():
					return nil, req.Context().Err()
				case <-time.After(backoffs[attempt]):
				}
				continue
			}
			return nil, err
		}

		// Check if this is a retryable status code
		if retryableStatusCodes[resp.StatusCode] && attempt < maxAttempts-1 {
			resp.Body.Close()
			lastResp = nil
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			select {
			case <-req.Context().Done():
				return nil, req.Context().Err()
			case <-time.After(backoffs[attempt]):
			}
			continue
		}

		return resp, nil
	}

	if lastResp != nil {
		return lastResp, nil
	}
	return nil, lastErr
}

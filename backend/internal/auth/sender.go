package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// HTTPSMSSender delivers login codes through a generic HTTP SMS gateway:
// POST {url} with a JSON body {"phone","code"} and the configured bearer
// token. Any 2xx reply counts as delivered; anything else fails the request
// so the caller can retry. It is the P0 provider implementation — swap it
// for a cloud-vendor client behind the same SMSSender interface if needed.
type HTTPSMSSender struct {
	url    string
	token  string
	client *http.Client
}

// SenderConfig carries the HTTP gateway settings read from configuration.
type SenderConfig struct {
	URL   string
	Token string
}

// NewHTTPSMSSender validates the gateway configuration.
func NewHTTPSMSSender(config SenderConfig) (*HTTPSMSSender, error) {
	if config.URL == "" {
		return nil, errors.New("auth: sms sender url is required")
	}
	return &HTTPSMSSender{
		url:    config.URL,
		token:  config.Token,
		client: &http.Client{Timeout: 5 * time.Second},
	}, nil
}

func (s *HTTPSMSSender) SendLoginCode(ctx context.Context, phone, code string) error {
	body, err := json.Marshal(map[string]string{"phone": phone, "code": code})
	if err != nil {
		return fmt.Errorf("auth: encode sms payload: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("auth: build sms request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	if s.token != "" {
		request.Header.Set("Authorization", "Bearer "+s.token)
	}

	response, err := s.client.Do(request)
	if err != nil {
		return fmt.Errorf("auth: send sms: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return fmt.Errorf("auth: sms gateway returned status %d", response.StatusCode)
	}
	return nil
}

var _ SMSSender = (*HTTPSMSSender)(nil)

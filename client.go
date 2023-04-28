package tdGo

import (
	"fmt"
	"github.com/go-resty/resty/v2"
	"log"
	"net/url"
)

// Client is td-go client
type Client struct {
	// apikey is Treasure Data API Key
	apikey string
	// baseURL is Treasure Data API Endpoint
	baseURL *url.URL
	// logger is LevelLogger
	logger *LevelLogger
	// httpClient is resty client
	httpClient *resty.Client
}

// Option is for Client Options of httpclient and logger
type Option func(*Client)

// NewClient is for create td-go client
func NewClient(apikey string, baseUrl string, options ...Option) (*Client, error) {
	parseUrl, err := url.Parse(baseUrl)
	r := resty.New()
	if err != nil {
		return nil, err
	}
	client := &Client{
		apikey:     apikey,
		baseURL:    parseUrl,
		httpClient: r,
		logger:     NewLevelLogger(WARNING, log.Default()),
	}

	for _, opt := range options {
		opt(client)
	}
	client.setMiddleware()
	return client, nil
}

// WithHttpClient is for Custom Http Client Option
func WithHttpClient(client *resty.Client) func(*Client) {
	return func(cli *Client) {
		cli.httpClient = client
	}
}

// WithLogger is for Custom Logger Option
func WithLogger(logger *LevelLogger) func(*Client) {
	return func(client *Client) {
		client.logger = logger
	}
}

const tdGoVersion = "0.0.1"

var UserAgent = fmt.Sprintf("td-go-v%s", tdGoVersion)

type Header map[string]string

// setMiddleware is for set middleware of httpclient
func (c *Client) setMiddleware() {
	c.httpClient.OnBeforeRequest(func(client *resty.Client, request *resty.Request) error {
		header := map[string]string{
			"Authorization": fmt.Sprintf("TD1 %s", c.apikey),
			"user-agent":    UserAgent,
		}
		request.SetHeaders(header)

		c.logger.Debugf("Request URL: %s", request.URL)
		c.logger.Debugf("Request Method: %s", request.Method)
		c.logger.Debugf("Request Headers: %v", request.Header)

		return nil
	})

	c.httpClient.OnAfterResponse(func(client *resty.Client, response *resty.Response) error {
		c.logger.Debugf("Response Status: %s", response.Status())
		c.logger.Debugf("Response Time: %v", response.Time())
		c.logger.Debugf("Response Headers: %v", response.Header())

		if response.IsError() {
			c.logger.Errorf("Response Error: %s", string(response.Body()))
		} else {
			c.logger.Debugf("Response Body: %s", string(response.Body()))
		}
		return nil
	})
}

// checkStatus is for check status code
func checkStatus(resp *resty.Response) error {
	if resp.StatusCode() >= 200 && resp.StatusCode() <= 299 {
		return nil
	} else {
		return fmt.Errorf("status code: %d, error: %s", resp.StatusCode(), resp.Body())
	}
}

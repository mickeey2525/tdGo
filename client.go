package tdGo

import (
	"fmt"
	"github.com/go-resty/resty/v2"
	"log"
	"net/url"
)

type Client struct {
	apikey     string
	baseURL    *url.URL
	logger     *log.Logger
	httpClient *resty.Client
}

type Option func(*Client)

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
	}

	for _, opt := range options {
		opt(client)
	}
	return client, nil
}

// WithHttpClient is for Custom Http Client Option
func WithHttpClient(client *resty.Client) func(*Client) {
	return func(cli *Client) {
		cli.httpClient = client
	}
}

// WithLogger is for Custom Logger Option
func WithLogger(logger *log.Logger) func(*Client) {
	return func(client *Client) {
		client.logger = logger
	}
}

const tdGoVersion = "0.0.1"

var UserAgent = fmt.Sprintf("td-go-v%s", tdGoVersion)

type Header map[string]string

// Set authentication and user agent
func (c *Client) setHeaders(req *resty.Client) {
	header := map[string]string{"Authorization": fmt.Sprintf("TD1 %s", c.apikey)}
	header["user-agent"] = UserAgent
	req.SetHeaders(header)
}

func checkStatus(resp *resty.Response) error {
	if resp.StatusCode() >= 200 && resp.StatusCode() <= 299 {
		return nil
	} else {
		return fmt.Errorf("status code: %d, error: %s", resp.StatusCode(), resp.Body())
	}
}

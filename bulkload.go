package tdGo

import (
	"context"
	"fmt"
)

// GetBulkloads returns json string as it is because fields are not fixed
func (c *Client) GetBulkloads(ctx context.Context) (string, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).Get(c.baseURL.String() + "/v3/bulk_loads")
	if err != nil {
		return "", err
	}
	if resp.IsError() {
		return "", fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return "", ok
	}
	return resp.String(), nil
}

// config map should look like below
// {
//   "config":{
//      "in":{
//         "type":"s3",
//         "bucket":"your-bucket",
//         "path_prefix":"logs/csv-",
//         "access_key_id":"YOUR-AWS-ACCESS-KEY",
//         "secret_access_key":"YOUR-AWS-SECRET-KEY"
//      },
//      "out":{
//         "mode":"append"
//      },
//      "exec":{
//         "guess_plugins":[
//            "json",
//            "query_string"
//         ]
//      }
//   }
//}

// GuessConfig generates whole json file
func (c *Client) GuessConfig(ctx context.Context, config string) (string, error) {
	c.setHeaders(c.httpClient)
	c.httpClient.SetHeader("content-type", "application/json")
	resp, err := c.httpClient.R().SetContext(ctx).SetBody(config).Post(c.baseURL.String() + "/v3/bulk_loads/guess")
	if err != nil {
		return "", err
	}
	if resp.IsError() {
		return "", fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return "", ok
	}
	return resp.String(), nil
}

func (c *Client) GetBulkLoadWithName(ctx context.Context, name string) (string, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).Get(c.baseURL.String() + fmt.Sprintf("/v3/bulk_loads/%s", name))
	if err != nil {
		return "", err
	}
	if resp.IsError() {
		return "", fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return "", ok
	}
	return resp.String(), nil
}

func (c *Client) GetBulkLoadJobs(ctx context.Context, name string) (string, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).Get(c.baseURL.String() + fmt.Sprintf("/v3/bulk_loads/%s/jobs", name))
	if err != nil {
		return "", err
	}
	if resp.IsError() {
		return "", fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return "", ok
	}
	return resp.String(), nil
}

func (c *Client) BulkLoadPreview(ctx context.Context, config string) (string, error) {
	c.setHeaders(c.httpClient)
	c.httpClient.SetHeader("content-type", "application/json")
	resp, err := c.httpClient.R().SetContext(ctx).SetBody(config).Post(c.baseURL.String() + "/v3/bulk_loads/preview")
	if err != nil {
		return "", err
	}
	if resp.IsError() {
		return "", fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return "", ok
	}
	return resp.String(), nil
}

func (c *Client) BulkLoadIssue(ctx context.Context, db, table, config string) (string, error) {
	c.setHeaders(c.httpClient)
	params := make(map[string]string)
	params["database"] = db
	params["table"] = table
	c.httpClient.SetHeader("content-type", "application/json")
	resp, err := c.httpClient.R().SetContext(ctx).SetQueryParams(params).SetBody(config).Post(c.baseURL.String() + "/v3/bulk_loads/preview")
	if err != nil {
		return "", err
	}
	if resp.IsError() {
		return "", fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return "", ok
	}
	return resp.String(), nil
}

type BulkLoadOptions struct {
}

func (c *Client) BulkLoadCreate(ctx context.Context, db, table, config string) (string, error) {
	c.setHeaders(c.httpClient)
	params := make(map[string]string)
	params["database"] = db
	params["table"] = table
	c.httpClient.SetHeader("content-type", "application/json")
	resp, err := c.httpClient.R().SetContext(ctx).SetQueryParams(params).SetBody(config).Post(c.baseURL.String() + "/v3/bulk_loads/preview")
	if err != nil {
		return "", err
	}
	if resp.IsError() {
		return "", fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return "", ok
	}
	return resp.String(), nil
}

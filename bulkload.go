package tdGo

import (
	"context"
	"fmt"
	"strconv"
)

// GetBulkLoad returns JSON array as *[]map[string]any because data format is flexible
func (c *Client) GetBulkLoad(ctx context.Context) (*[]map[string]any, error) {

	var result []map[string]any
	resp, err := c.httpClient.R().SetResult(result).SetContext(ctx).Get(c.baseURL.String() + "/v3/bulk_loads")
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return nil, ok
	}
	return resp.Result().(*[]map[string]any), nil
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
func (c *Client) GuessConfig(ctx context.Context, config string) (*map[string]any, error) {
	var result map[string]any

	c.httpClient.SetHeader("content-type", "application/json")
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(result).SetBody(config).Post(c.baseURL.String() + "/v3/bulk_loads/guess")
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return nil, ok
	}
	return resp.Result().(*map[string]any), nil
}

// GetBulkLoadWithName will return BulkLoad session by specifying unique id like s3_v2_import_xyz
func (c *Client) GetBulkLoadWithName(ctx context.Context, name string) (*map[string]any, error) {
	var result map[string]any

	resp, err := c.httpClient.R().SetResult(result).SetContext(ctx).Get(c.baseURL.String() + fmt.Sprintf("/v3/bulk_loads/%s", name))
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return nil, ok
	}
	return resp.Result().(*map[string]any), nil
}

// GetBulkLoadHistory will return BulkLoad history by specifying unique name like s3_v2_import_xyz
func (c *Client) GetBulkLoadHistory(ctx context.Context, name string) (*[]map[string]any, error) {
	var result []map[string]any

	resp, err := c.httpClient.R().SetContext(ctx).SetResult(result).Get(c.baseURL.String() + fmt.Sprintf("/v3/bulk_loads/%s/jobs", name))
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return nil, ok
	}
	return resp.Result().(*[]map[string]any), nil
}

// BulkLoadPreview will return BulkLoad preview by specifying configs
func (c *Client) BulkLoadPreview(ctx context.Context, config map[string]any) (*map[string]any, error) {
	var result map[string]any
	c.httpClient.SetHeader("content-type", "application/json")
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(result).SetBody(config).Post(c.baseURL.String() + "/v3/bulk_loads/preview")
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return nil, ok
	}
	return resp.Result().(*map[string]any), nil
}

// BulkLoadIssue will run a BulkLoad job by specifying config
func (c *Client) BulkLoadIssue(ctx context.Context, db, table string, config map[string]any) (*JobInfo, error) {
	params := make(map[string]string)
	params["database"] = db
	params["table"] = table
	c.httpClient.SetHeader("content-type", "application/json")
	resp, err := c.httpClient.R().SetContext(ctx).SetQueryParams(params).SetResult(&JobInfo{}).SetBody(config).Post(c.baseURL.String() + fmt.Sprintf("/v3/job/issue/bulkload/%s", db))

	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return nil, ok
	}
	return resp.Result().(*JobInfo), nil
}

// BulkLoadOption is Source's config.
type BulkLoadOption struct {
	// Name is Source name
	Name string
	// Cron is a Schedule of the Source
	// Should be @daily, @hourly, "10 * * * *"
	Cron string
	// Timezone of the Schedule
	// For example, Asia/Tokyo
	Timezone string
	// Delay ensures all buffered events are imported
	// before running the query. Default: 0
	Delay int
	// TimeColumn is used to specify the time column
	TimeColumn string
}

// BulkLoadCreate creates Source Settings using config parameter
func (c *Client) BulkLoadCreate(ctx context.Context, db, table string, config map[string]any, option BulkLoadOption) (string, error) {

	params := make(map[string]string)
	params["database"] = db
	params["table"] = table
	params["name"] = option.Name
	params["cron"] = option.Cron
	params["timezone"] = option.Timezone
	params["time_column"] = option.TimeColumn
	params["delay"] = strconv.Itoa(option.Delay)
	c.httpClient.SetHeader("content-type", "application/json")
	resp, err := c.httpClient.R().SetContext(ctx).SetQueryParams(params).SetBody(config).Post(c.baseURL.String() + "/v3/bulk_loads")
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

// BulkLoadUpdate updates Source Settings using config parameter
func (c *Client) BulkLoadUpdate(ctx context.Context, name string, config map[string]any) (string, error) {
	resp, err := c.httpClient.R().SetContext(ctx).SetBody(config).SetContentLength(true).Put(c.baseURL.String() + fmt.Sprintf("/v3/bulk_loads/%s", name))
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

// BulkLoadDelete deletes Source Settings
func (c *Client) BulkLoadDelete(ctx context.Context, name string) (string, error) {
	resp, err := c.httpClient.R().SetContext(ctx).Delete(c.baseURL.String() + fmt.Sprintf("/v3/bulk_loads/%s", name))
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

type RunResponse struct {
	JobId int `json:"job_id"`
}

// BulkLoadRun runs a BulkLoad job by specifying BulkLoad name like like s3_v2_import_xyz
func (c *Client) BulkLoadRun(ctx context.Context, name string) (*RunResponse, error) {
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&RunResponse{}).Get(c.baseURL.String() + fmt.Sprintf("/v3/bulk_loads/%s/jobs", name))
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return nil, ok
	}
	return resp.Result().(*RunResponse), nil
}

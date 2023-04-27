package tdGo

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

type JobList struct {
	Count int   `json:"count"`
	From  int   `json:"from"`
	To    int   `json:"to"`
	Jobs  []Job `json:"jobs"`
}

type Job struct {
	JobId                   string      `json:"job_id"`
	CpuTime                 interface{} `json:"cpu_time"`
	CreatedAt               string      `json:"created_at"`
	Duration                int         `json:"duration"`
	EndAt                   string      `json:"end_at"`
	NumRecords              int         `json:"num_records"`
	ResultSize              int         `json:"result_size"`
	StartAt                 string      `json:"start_at"`
	Status                  string      `json:"status"`
	UpdatedAt               string      `json:"updated_at"`
	Database                string      `json:"database"`
	HiveResultSchema        string      `json:"hive_result_schema"`
	LinkedResultExportJobId interface{} `json:"linked_result_export_job_id"`
	Organization            interface{} `json:"organization"`
	Priority                int         `json:"priority"`
	Query                   interface{} `json:"query"`
	Result                  string      `json:"result"`
	ResultExportTargetJobId interface{} `json:"result_export_target_job_id"`
	RetryLimit              int         `json:"retry_limit"`
	Type                    string      `json:"type"`
	Url                     string      `json:"url"`
	UserName                string      `json:"user_name"`
}

// JobInfo when you issue job, this json will be returned
type JobInfo struct {
	Job      string `json:"job"`
	Database string `json:"database"`
	JobId    string `json:"job_id"`
}

// JobDetails of /v3/job/show API
type JobDetails struct {
	Job   Job   `json:"job"`
	Debug Debug `json:"debug"`
}

type Debug struct {
	Cmdout interface{} `json:"cmdout"`
	Stderr interface{} `json:"stderr"`
}

func (c *Client) GetJobList(ctx context.Context, from, to int, status JobStatus) (*JobList, error) {
	queryParams := map[string]string{
		"from": strconv.Itoa(from),
		"to":   strconv.Itoa(to),
	}
	// status must be lowercase
	if status != All {
		queryParams["status"] = strings.ToLower(status.String())
	}
	client := c.httpClient
	c.setHeaders(client)
	resp, err := client.R().SetContext(ctx).SetQueryParams(queryParams).SetResult(&JobList{}).Get(c.baseURL.String() + "/v3/job/list")
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

	return resp.Result().(*JobList), nil
}

func (c *Client) ShowJob(ctx context.Context, jobId int) (*JobDetails, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&JobDetails{}).Get(c.baseURL.String() + fmt.Sprintf("/v3/job/show/%d", jobId))
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
	return resp.Result().(*JobDetails), nil
}

type JobOption struct {
	Query      string `json:"query"`
	Priority   int    `json:"priority"`
	ResultUrl  string `json:"result_url"`
	RetryLimit int    `json:"retry_limit"`
	PoolName   string `json:"pool_name"`
}

func (c *Client) CreateJob(ctx context.Context, jobType JobType, database string, jobOption JobOption) (*JobInfo, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&JobInfo{}).SetBody(jobOption).Post(c.baseURL.String() + fmt.Sprintf("/v3/job/issue/%s/%s", strings.ToLower(jobType.String()), database))
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

type JobSts struct {
	JobId      string      `json:"job_id"`
	CpuTime    interface{} `json:"cpu_time"`
	CreatedAt  string      `json:"created_at"`
	Duration   int         `json:"duration"`
	EndAt      string      `json:"end_at"`
	NumRecords int         `json:"num_records"`
	ResultSize int         `json:"result_size"`
	StartAt    string      `json:"start_at"`
	Status     string      `json:"status"`
	UpdatedAt  string      `json:"updated_at"`
}

func (c *Client) CheckJobStatus(ctx context.Context, jobId int) (*JobSts, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&JobSts{}).Get(c.baseURL.String() + fmt.Sprintf("/v3/job/status/%d", jobId))
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
	return resp.Result().(*JobSts), nil
}

func (c *Client) KillJob(ctx context.Context, jobId int) error {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).Post(c.baseURL.String() + fmt.Sprintf("/v3/job/kill/%d", jobId))
	if err != nil {
		return err
	}
	if resp.IsError() {
		return fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return ok
	}
	return nil
}

func (c *Client) GetResult(ctx context.Context, jobId int, format FileFormat, outPath string) error {
	c.setHeaders(c.httpClient)
	formatTmp := strings.ToLower(format.String())
	if formatTmp == "msgpackgz" {
		formatTmp = "msgpack.gz"
	}
	c.httpClient.SetQueryParam("format", formatTmp)
	resp, err := c.httpClient.R().SetContext(ctx).SetOutput(outPath).Get(c.baseURL.String() + fmt.Sprintf("/v3/job/result/%d", jobId))
	if err != nil {
		return err
	}
	if resp.IsError() {
		return fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return ok
	}
	return nil
}

func (c *Client) SetResultExport(ctx context.Context, jobId int, resultExportSettings string) (*Job, error) {
	c.setHeaders(c.httpClient)
	resultSettings := make(map[string]string)
	resultSettings["result"] = resultExportSettings
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&Job{}).SetBody(resultSettings).Post(c.baseURL.String() + fmt.Sprintf("/v3/job/result_export/%d", jobId))
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
	return resp.Result().(*Job), nil
}

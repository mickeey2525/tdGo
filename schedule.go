package tdGo

import (
	"context"
	"errors"
	"fmt"
	"strconv"
)

// ScheduleList is a List of Schedule
type ScheduleList struct {
	Schedules []Schedule `json:"schedules"`
}

// Schedule is a struct of Schedule Query
type Schedule struct {
	// Name is a name of schedule query
	Name string `json:"name"`
	// Cron is a cron expression.This can be nil when schedule is not set.
	Cron any `json:"cron"`
	// Timezone is a timezone of schedule query
	Timezone string `json:"timezone"`
	// Delay is a delay of schedule query
	Delay int `json:"delay"`
	// CreatedAt is a created time of schedule query
	CreatedAt string `json:"created_at"`
	// Type is a type of schedule query
	// Presto or Hive
	Type JobType `json:"type"`
	// Query is a query of schedule query
	Query string `json:"query"`
	// Database is a database name of schedule query is set.
	// Database is used to refer to the users permissions.
	Database string `json:"database"`
	// UserName is a username of schedule query writer.
	UserName string `json:"user_name"`
	// Priority is a priority of schedule query
	// see https://docs.treasuredata.com/display/public/PD/Setting+Job+Priority
	Priority   int `json:"priority"`
	RetryLimit int `json:"retry_limit"`
	// Result is result output settings.
	Result   string `json:"result"`
	NextTime any    `json:"next_time"`
}

func (c *Client) GetSchedules(ctx context.Context) (*ScheduleList, error) {

	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&ScheduleList{}).Get(c.baseURL.String() + "/v3/schedule/list")
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
	return resp.Result().(*ScheduleList), nil
}

type ScheduleOption struct {
	Cron         string
	Database     string
	Query        string
	Result       string
	Timezone     string
	ResourcePool string
	Delay        int
	RetryLimit   int
	Priority     int
	QueryType    JobType
}

func (c *Client) CreateSchedule(ctx context.Context, name string, options ScheduleOption) (*Schedule, error) {

	body := make(map[string]string)
	body["cron"] = options.Cron
	if options.Database == "" {
		return nil, errors.New("database must not empty")
	}
	body["database"] = options.Database
	if options.Query == "" {
		return nil, errors.New("query must not empty")
	}
	body["query"] = options.Query
	if options.ResourcePool != "" {
		body["pool_name"] = options.ResourcePool
	}
	body["priority"] = strconv.Itoa(options.Priority)
	body["retry_limit"] = strconv.Itoa(options.RetryLimit)
	body["delay"] = strconv.Itoa(options.Delay)
	body["result"] = options.Result
	if options.Timezone == "" {
		body["timezone"] = "UTC"
	} else {
		body["timezone"] = options.Timezone
	}

	resp, err := c.httpClient.R().SetContext(ctx).SetFormData(body).SetResult(&Schedule{}).Post(c.baseURL.String() + fmt.Sprintf("/v3/schedule/create/%s", name))
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
	return resp.Result().(*Schedule), nil
}

func (c *Client) DeleteSchedule(ctx context.Context, name string) (*Schedule, error) {

	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&Schedule{}).Post(c.baseURL.String() + fmt.Sprintf("/v3/schedule/delete/%s", name))
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
	return resp.Result().(*Schedule), nil
}

func (c *Client) UpdateSchedule(ctx context.Context, name string, options ScheduleOption) (*Schedule, error) {

	c.httpClient.SetHeader("content-type", "application/x-www-form-urlencoded")
	body := make(map[string]string)
	body["cron"] = options.Cron
	if options.Database == "" {
		return nil, errors.New("database must not empty")
	}
	body["database"] = options.Database
	if options.Query == "" {
		return nil, errors.New("query must not empty")
	}
	body["query"] = options.Query
	if options.ResourcePool != "" {
		body["pool_name"] = options.ResourcePool
	}
	body["priority"] = strconv.Itoa(options.Priority)
	body["retry_limit"] = strconv.Itoa(options.RetryLimit)
	body["delay"] = strconv.Itoa(options.Delay)
	body["result"] = options.Result
	if options.Timezone == "" {
		body["timezone"] = "UTC"
	} else {
		body["timezone"] = options.Timezone
	}

	resp, err := c.httpClient.R().SetContext(ctx).SetFormData(body).SetResult(&Schedule{}).Post(c.baseURL.String() + fmt.Sprintf("/v3/schedule/update/%s", name))
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
	return resp.Result().(*Schedule), nil
}

type SchedJobs struct {
	Jobs []struct {
		JobId       int    `json:"job_id"`
		ScheduledAt string `json:"scheduled_at"`
		Type        string `json:"type"`
	} `json:"jobs"`
}

func (c *Client) RunSchedule(ctx context.Context, name string, scheduleTime int64) (*SchedJobs, error) {

	c.httpClient.SetHeader("content-type", "application/x-www-form-urlencoded")
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&SchedJobs{}).Post(c.baseURL.String() + fmt.Sprintf("/v3/schedule/run/%s/%d", name, scheduleTime))
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
	return resp.Result().(*SchedJobs), nil

}

type SchedHistory struct {
	Count   int   `json:"count"`
	From    int   `json:"from"`
	To      int   `json:"to"`
	History []Job `json:"history"`
}

func (c *Client) GetScheduleHistory(ctx context.Context, name string, from, to int) (*SchedHistory, error) {

	params := make(map[string]string)
	params["from"] = strconv.Itoa(from)
	params["to"] = strconv.Itoa(to)
	c.httpClient.SetQueryParams(params)
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&SchedHistory{}).Get(c.baseURL.String() + fmt.Sprintf("/v3/schedule/history/%s", name))
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
	return resp.Result().(*SchedHistory), nil
}

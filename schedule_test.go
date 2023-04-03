package tdGo

import (
	"context"
	"github.com/go-resty/resty/v2"
	"log"
	"net/url"
	"reflect"
	"testing"
)

func TestClient_CreateSchedule(t *testing.T) {
	type fields struct {
		apikey     string
		baseURL    *url.URL
		logger     *log.Logger
		httpClient *resty.Client
	}
	type args struct {
		ctx     context.Context
		name    string
		options ScheduleOption
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Schedule
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				apikey:     tt.fields.apikey,
				baseURL:    tt.fields.baseURL,
				logger:     tt.fields.logger,
				httpClient: tt.fields.httpClient,
			}
			got, err := c.CreateSchedule(tt.args.ctx, tt.args.name, tt.args.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateSchedule() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CreateSchedule() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_DeleteSchedule(t *testing.T) {
	type fields struct {
		apikey     string
		baseURL    *url.URL
		logger     *log.Logger
		httpClient *resty.Client
	}
	type args struct {
		ctx  context.Context
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Schedule
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				apikey:     tt.fields.apikey,
				baseURL:    tt.fields.baseURL,
				logger:     tt.fields.logger,
				httpClient: tt.fields.httpClient,
			}
			got, err := c.DeleteSchedule(tt.args.ctx, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeleteSchedule() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DeleteSchedule() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_GetScheduleHistory(t *testing.T) {
	type fields struct {
		apikey     string
		baseURL    *url.URL
		logger     *log.Logger
		httpClient *resty.Client
	}
	type args struct {
		ctx  context.Context
		name string
		from int
		to   int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *SchedHistory
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				apikey:     tt.fields.apikey,
				baseURL:    tt.fields.baseURL,
				logger:     tt.fields.logger,
				httpClient: tt.fields.httpClient,
			}
			got, err := c.GetScheduleHistory(tt.args.ctx, tt.args.name, tt.args.from, tt.args.to)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetScheduleHistory() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetScheduleHistory() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_GetSchedules(t *testing.T) {
	type fields struct {
		apikey     string
		baseURL    *url.URL
		logger     *log.Logger
		httpClient *resty.Client
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *ScheduleList
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				apikey:     tt.fields.apikey,
				baseURL:    tt.fields.baseURL,
				logger:     tt.fields.logger,
				httpClient: tt.fields.httpClient,
			}
			got, err := c.GetSchedules(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetSchedules() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetSchedules() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_RunSchedule(t *testing.T) {
	type fields struct {
		apikey     string
		baseURL    *url.URL
		logger     *log.Logger
		httpClient *resty.Client
	}
	type args struct {
		ctx          context.Context
		name         string
		scheduleTime int64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *SchedJobs
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				apikey:     tt.fields.apikey,
				baseURL:    tt.fields.baseURL,
				logger:     tt.fields.logger,
				httpClient: tt.fields.httpClient,
			}
			got, err := c.RunSchedule(tt.args.ctx, tt.args.name, tt.args.scheduleTime)
			if (err != nil) != tt.wantErr {
				t.Errorf("RunSchedule() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RunSchedule() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_UpdateSchedule(t *testing.T) {
	type fields struct {
		apikey     string
		baseURL    *url.URL
		logger     *log.Logger
		httpClient *resty.Client
	}
	type args struct {
		ctx     context.Context
		name    string
		options ScheduleOption
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Schedule
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				apikey:     tt.fields.apikey,
				baseURL:    tt.fields.baseURL,
				logger:     tt.fields.logger,
				httpClient: tt.fields.httpClient,
			}
			got, err := c.UpdateSchedule(tt.args.ctx, tt.args.name, tt.args.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("UpdateSchedule() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("UpdateSchedule() got = %v, want %v", got, tt.want)
			}
		})
	}
}

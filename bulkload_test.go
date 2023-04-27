package tdGo

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-resty/resty/v2"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

func TestClient_BulkLoadCreate(t *testing.T) {
	// test if the path and method are correct
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/bulk_loads") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		if r.Method != "POST" {
			log.Fatalln("Method must be POST")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `{"id":391789,"name":"test_bulk_load","cron":"","timezone":"UTC","delay":0,"start_at":null,"database":"tachibana_s3_testdb","table":"tdgo_test","config":{"in":{"region":"ap-northeast-1","auth_method":"assume_role","td_instance_profile":"arn:aws:iam::523683666290:root","account_id":"672801353311","role_name":"sample_td_assume_role","external_id":"***","duration_in_seconds":3600,"bucket":"my-tdsupport-tf-sample-test","path_prefix":"1465868324.csv","parser":{"allow_extra_columns":false,"allow_optional_columns":false,"charset":"UTF-8","columns":[{"name":"host","type":"string"},{"name":"path","type":"string"},{"name":"method","type":"string"},{"name":"referer","type":"string"},{"name":"code","type":"long"},{"name":"agent","type":"string"},{"name":"user","type":"string"},{"name":"size","type":"long"},{"name":"time","type":"long"}],"delimiter":",","escape":"\"","newline":"LF","quote":"\"","skip_header_lines":1,"trim_if_not_quoted":false,"type":"csv"},"type":"s3_v2"},"filters":[],"exec":{},"out":{"type":"td_internal","mode":"append","plazma_dataset":"10452/tachibana_s3_testdb/tdgo_test_20230427_075523_4b28b415"}},"config_diff":{}}`)
		if err != nil {
			return
		}
	}))
	defer ts.Close()

	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	option := BulkLoadOption{
		Name:       "test_bulk_load",
		Cron:       "",
		Timezone:   "UTC",
		Delay:      0,
		TimeColumn: "",
	}
	var config map[string]any
	configString := `
	{
		  "config": {
		    "in": {
		      "type": "s3_v2",
		      "td_authentication_id": 269100,
		      "bucket": "my-tdsupport-tf-sample-test",
		      "path_prefix": "1465868324.csv"
		    },
		    "out": {
		      "mode": "append"
		    }
		  }
		}
	`
	json.Unmarshal([]byte(configString), &config)
	got, err := client.BulkLoadCreate(context.Background(), "test_bulk_load", "test_bulk_load", config, option)
	if err != nil {
		log.Fatalln(err)
	}
	want := `{"id":391789,"name":"test_bulk_load","cron":"","timezone":"UTC","delay":0,"start_at":null,"database":"tachibana_s3_testdb","table":"tdgo_test","config":{"in":{"region":"ap-northeast-1","auth_method":"assume_role","td_instance_profile":"arn:aws:iam::523683666290:root","account_id":"672801353311","role_name":"sample_td_assume_role","external_id":"***","duration_in_seconds":3600,"bucket":"my-tdsupport-tf-sample-test","path_prefix":"1465868324.csv","parser":{"allow_extra_columns":false,"allow_optional_columns":false,"charset":"UTF-8","columns":[{"name":"host","type":"string"},{"name":"path","type":"string"},{"name":"method","type":"string"},{"name":"referer","type":"string"},{"name":"code","type":"long"},{"name":"agent","type":"string"},{"name":"user","type":"string"},{"name":"size","type":"long"},{"name":"time","type":"long"}],"delimiter":",","escape":"\"","newline":"LF","quote":"\"","skip_header_lines":1,"trim_if_not_quoted":false,"type":"csv"},"type":"s3_v2"},"filters":[],"exec":{},"out":{"type":"td_internal","mode":"append","plazma_dataset":"10452/tachibana_s3_testdb/tdgo_test_20230427_075523_4b28b415"}},"config_diff":{}}`
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Client.BulkLoadCreate() = %v, want %v", got, want)
	}
}

func TestClient_BulkLoadDelete(t *testing.T) {
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
		want    string
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
			got, err := c.BulkLoadDelete(tt.args.ctx, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("BulkLoadDelete() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("BulkLoadDelete() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_BulkLoadIssue(t *testing.T) {
	type fields struct {
		apikey     string
		baseURL    *url.URL
		logger     *log.Logger
		httpClient *resty.Client
	}
	type args struct {
		ctx    context.Context
		db     string
		table  string
		config map[string]any
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *JobInfo
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
			got, err := c.BulkLoadIssue(tt.args.ctx, tt.args.db, tt.args.table, tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("BulkLoadIssue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BulkLoadIssue() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_BulkLoadPreview(t *testing.T) {
	type fields struct {
		apikey     string
		baseURL    *url.URL
		logger     *log.Logger
		httpClient *resty.Client
	}
	type args struct {
		ctx    context.Context
		config map[string]any
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *map[string]any
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
			got, err := c.BulkLoadPreview(tt.args.ctx, tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("BulkLoadPreview() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BulkLoadPreview() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_BulkLoadRun(t *testing.T) {
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
		want    *RunResponse
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
			got, err := c.BulkLoadRun(tt.args.ctx, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("BulkLoadRun() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("BulkLoadRun() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_BulkLoadUpdate(t *testing.T) {
	type fields struct {
		apikey     string
		baseURL    *url.URL
		logger     *log.Logger
		httpClient *resty.Client
	}
	type args struct {
		ctx    context.Context
		name   string
		config map[string]any
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
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
			got, err := c.BulkLoadUpdate(tt.args.ctx, tt.args.name, tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("BulkLoadUpdate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("BulkLoadUpdate() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_GetBulkLoad(t *testing.T) {
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
		want    *[]map[string]any
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
			got, err := c.GetBulkLoad(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetBulkLoad() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetBulkLoad() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_GetBulkLoadWithName(t *testing.T) {
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
		want    *map[string]any
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
			got, err := c.GetBulkLoadWithName(tt.args.ctx, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetBulkLoadWithName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetBulkLoadWithName() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_GuessConfig(t *testing.T) {
	type fields struct {
		apikey     string
		baseURL    *url.URL
		logger     *log.Logger
		httpClient *resty.Client
	}
	type args struct {
		ctx    context.Context
		config string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *map[string]any
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
			got, err := c.GuessConfig(tt.args.ctx, tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("GuessConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GuessConfig() got = %v, want %v", got, tt.want)
			}
		})
	}
}

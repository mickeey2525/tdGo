package tdGo

import (
	"github.com/go-resty/resty/v2"
	"log"
	"net/url"
	"reflect"
	"testing"
)

func TestClient_New(t *testing.T) {
	base, _ := url.Parse("https://api.treasuredata.com")
	type fields struct {
		apikey     string
		baseURL    *url.URL
		logger     *log.Logger
		httpClient *resty.Client
	}
	type args struct {
		apikey  string
		baseUrl string
		options []Option
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Client
		wantErr bool
	}{
		{
			name: "default client",
			fields: fields{
				apikey:     "token",
				baseURL:    base,
				logger:     nil,
				httpClient: resty.New(),
			},
			args: args{
				apikey:  "token",
				baseUrl: "https://api.treasuredata.com",
				options: nil,
			},
			want: &Client{
				apikey:     "token",
				baseURL:    base,
				httpClient: resty.New(),
			},
		},
		{
			name: "default client",
			fields: fields{
				apikey:     "token",
				baseURL:    base,
				logger:     nil,
				httpClient: resty.New(),
			},
			args: args{
				apikey:  "token",
				baseUrl: "https://api.treasuredata.com",
				options: nil,
			},
			want: &Client{
				apikey:     "token",
				baseURL:    base,
				httpClient: resty.New(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewClient(tt.args.apikey, tt.args.baseUrl, tt.args.options...)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got.apikey == tt.want.apikey && !reflect.DeepEqual(got.baseURL, tt.want.baseURL) && !reflect.DeepEqual(got.httpClient, tt.want.httpClient) {
				t.Errorf("New() got = %+v, want %+v", got, tt.want)
			}
		})
	}
}

/*
func TestWithHttpClient(t *testing.T) {
	type args struct {
		client *resty.Client
	}
	tests := []struct {
		name string
		args args
		want func(*Client)
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := WithHttpClient(tt.args.client); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("WithHttpClient() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWithLogger(t *testing.T) {
	type args struct {
		logger *log.Logger
	}
	tests := []struct {
		name string
		args args
		want func(*Client)
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := WithLogger(tt.args.logger); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("WithLogger() = %v, want %v", got, tt.want)
			}
		})
	}
}
*/

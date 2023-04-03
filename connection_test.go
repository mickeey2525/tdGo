package tdGo

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestClient_CreateConnections(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `
	{
		"name": "tdGo_test_connection",
		"result": "tdGo_test_connection"
	}
	`)
		if err != nil {
			log.Fatalln(err)
		}
	}))
	defer ts.Close()
	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}

	connectionSettings := make(map[string]string)
	connectionSettings["type"] = "sftp"
	connectionSettings["host"] = "localhost"
	connectionSettings["port"] = "22"
	connectionName := "tdGo_test_connection"
	got, err := client.CreateConnections(context.Background(), connectionName, connectionSettings, "")
	if err != nil {
		log.Fatalln(err)
	}
	want := &ConnectionResult{
		Result: connectionName,
		Name:   connectionName,
	}

	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}

}

func TestClient_DeleteConnection(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `
	{
		"name": "tdGo_test_connection",
		"result": "tdGo_test_connection"
	}
	`)
		if err != nil {
			log.Fatalln(err)
		}
	}))
	defer ts.Close()
	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	connectionName := "tdGo_test_connection"
	got, err := client.DeleteConnection(context.Background(), "tdGo_test_connection")
	if err != nil {
		log.Fatalln(err)
	}
	want := &ConnectionResult{
		Result: connectionName,
		Name:   connectionName,
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_GetConnectionIdWithName(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `
{
  "id": 123456
}
	`)
		if err != nil {
			log.Fatalln(err)
		}
	}))
	defer ts.Close()
	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	got, err := client.GetConnectionIdWithName(context.Background(), "test")
	want := &ConnectionId{ID: 123456}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_GetConnections(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `
{
  "results": [
    {
      "name": "test",
      "url": "{\"type\":\"google_sheets\",\"mode\":\"replace\",\"range\":\"A1\",\"rows_threshold\":5000,\"value_input_option\":\"RAW\",\"set_nil_for_double_nan\":true}",
      "organization": null
    },
    {
      "name": "test2",
      "url": "{\"type\":\"elasticsearch\",\"nodes\":[{\"host\":\"aaaaaaa\",\"port\":9300}],\"cluster_name\":\"elasticsearch\",\"bulk_actions\":1000,\"bulk_size\":5242880,\"concurrent_requests\":5,\"maximum_retries\":3,\"maximum_retry_interval_millis\":120000}",
      "organization": null
    }
  ]
}
	`)
		if err != nil {
			log.Fatalln(err)
		}
	}))
	defer ts.Close()
	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	got, err := client.GetConnections(context.Background())
	if err != nil {
		log.Fatalln(err)
	}
	want := &Results{
		[]Result{
			{
				Name:         "test",
				Url:          "{\"type\":\"google_sheets\",\"mode\":\"replace\",\"range\":\"A1\",\"rows_threshold\":5000,\"value_input_option\":\"RAW\",\"set_nil_for_double_nan\":true}",
				Organization: nil,
			},
			{
				Name:         "test2",
				Url:          "{\"type\":\"elasticsearch\",\"nodes\":[{\"host\":\"aaaaaaa\",\"port\":9300}],\"cluster_name\":\"elasticsearch\",\"bulk_actions\":1000,\"bulk_size\":5242880,\"concurrent_requests\":5,\"maximum_retries\":3,\"maximum_retry_interval_millis\":120000}",
				Organization: nil,
			},
		},
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

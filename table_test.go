package tdGo

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestClient_ChangeDatabase(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/table/change_database/") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "POST" {
			log.Fatalln("Method must be POST")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `{"database": "Alpha","table": "Bravo","type": "log"}`)
		if err != nil {
			return
		}
	}))
	defer ts.Close()

	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	got, err := client.ChangeDatabase(context.Background(), "Alpha", "Bravo", "Omega")
	if err != nil {
		log.Fatalln(err)
	}
	want := &TableInfo{
		Database: "Alpha",
		Table:    "Bravo",
		Type:     "log",
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_CreateTable(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/table/create/") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "POST" {
			log.Fatalln("Method must be POST")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `{"database": "Alpha","table": "Bravo","type": "log"}`)
		if err != nil {
			return
		}
	}))
	defer ts.Close()

	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	got, err := client.CreateTable(context.Background(), "Alpha", "Bravo")
	if err != nil {
		log.Fatalln(err)
	}
	want := &TableInfo{
		Database: "Alpha",
		Table:    "Bravo",
		Type:     "log",
	}

	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_DeleteTable(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/table/delete") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "POST" {
			log.Fatalln("Method must be POST")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `{"database": "Alpha","table": "Bravo","type": "log"}`)
		if err != nil {
			return
		}
	}))
	defer ts.Close()

	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	got, err := client.DeleteTable(context.Background(), "Alpha", "Bravo")
	want := &TableInfo{
		Database: "Alpha",
		Table:    "Bravo",
		Type:     "log",
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_GetDistribution(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/table/distribution") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "GET" {
			log.Fatalln("Method must be GET")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `
{
    "bucket_count": 512,
    "columns": [
        {
            "key": "cdp_customer_id",
            "name": "cdp_customer_id",
            "type": "string"
        }
    ],
    "partition_function": "hash"
}`)
		if err != nil {
			return
		}
	}))
	defer ts.Close()

	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	got, err := client.GetDistribution(context.Background(), "Alpha", "Bravo")
	if err != nil {
		log.Fatalln(err)
	}
	want := &TableDistribution{
		BucketCount: 512,
		Columns: []Column{
			{
				Key:  "cdp_customer_id",
				Name: "cdp_customer_id",
				Type: "string",
			},
		},
		PartitionFunction: "hash",
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_GetTable(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/table/show") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "GET" {
			log.Fatalln("Method must be GET")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `
{
    "count": 8,
    "counter_updated_at": "2023-03-27T05:54:26Z",
    "created_at": "2021-04-14 07:03:31 UTC",
    "delete_protected": false,
    "estimated_storage_size": 0,
    "expire_days": null,
    "id": 121207442,
    "include_v": true,
    "last_log_timestamp": "2020-04-01T09:20:45Z",
    "name": "test",
    "schema": "[[\"c1\",\"string\",\"c1\"],[\"c0\",\"string\",\"c0\"]]",
    "type": "log",
    "updated_at": "2021-04-14 07:03:31 UTC"
}

`)
		if err != nil {
			return
		}
	}))
	defer ts.Close()

	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	got, err := client.GetTable(context.Background(), "test", "test")
	want := &Table{
		Count:                8,
		CounterUpdatedAt:     "2023-03-27T05:54:26Z",
		CreatedAt:            "2021-04-14 07:03:31 UTC",
		DeleteProtected:      false,
		EstimatedStorageSize: 0,
		ExpireDays:           nil,
		Id:                   121207442,
		IncludeV:             true,
		LastLogTimestamp:     "2020-04-01T09:20:45Z",
		Name:                 "test",
		Schema:               "[[\"c1\",\"string\",\"c1\"],[\"c0\",\"string\",\"c0\"]]",
		Type:                 "log",
		UpdatedAt:            "2021-04-14 07:03:31 UTC",
	}

	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_GetTablesList(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/table/list") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "GET" {
			log.Fatalln("Method must be GET")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `
{
  "database": "test_db",
  "tables": [
    {
      "id": 121207442,
      "name": "test",
      "estimated_storage_size": 0,
      "counter_updated_at": "2023-03-27T05:54:26Z",
      "last_log_timestamp": "2020-04-01T09:20:45Z",
      "delete_protected": false,
      "created_at": "2021-04-14 07:03:31 UTC",
      "updated_at": "2021-04-14 07:03:31 UTC",
      "type": "log",
      "include_v": true,
      "count": 8,
      "schema": "[[\"c1\",\"string\",\"c1\"],[\"c0\",\"string\",\"c0\"]]",
      "expire_days": null
    }
  ]
}
`)
		if err != nil {
			return
		}
	}))
	defer ts.Close()

	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	got, err := client.GetTablesList(context.Background(), "testDb")
	if err != nil {
		log.Fatalln(err)
	}
	want := &Tables{
		Database: "test_db",
		Tables: []Table{
			{
				Id:                   121207442,
				Name:                 "test",
				EstimatedStorageSize: 0,
				CounterUpdatedAt:     "2023-03-27T05:54:26Z",
				LastLogTimestamp:     "2020-04-01T09:20:45Z",
				DeleteProtected:      false,
				CreatedAt:            "2021-04-14 07:03:31 UTC",
				UpdatedAt:            "2021-04-14 07:03:31 UTC",
				Type:                 "log",
				IncludeV:             true,
				Count:                8,
				Schema:               "[[\"c1\",\"string\",\"c1\"],[\"c0\",\"string\",\"c0\"]]",
				ExpireDays:           nil,
			},
		},
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v \n, but got %+v", want, got)
	}
}

func TestClient_RenameTable(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/table/rename") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "POST" {
			log.Fatalln("Method must be POST")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `{"database": "testdb","table": "testtbl2","type": "log"}`)
		if err != nil {
			return
		}
	}))
	defer ts.Close()

	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	got, err := client.RenameTable(context.Background(), "testdb", "testtbl1", "testtbl2")
	if err != nil {
		log.Fatalln(err)
	}
	want := &TableInfo{
		Database: "testdb",
		Table:    "testtbl2",
		Type:     "log",
	}

	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_SwapTable(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/table/swap") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "POST" {
			log.Fatalln("Method must be POST")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `{"database": "testdb","table1": "test_tbl2","table2": "test_tbl1"}`)
		if err != nil {
			return
		}
	}))
	defer ts.Close()

	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	got, err := client.SwapTable(context.Background(), "testdb", "test_tbl1", "test_tbl2")
	want := &SwapInfo{
		Database: "testdb",
		Table1:   "test_tbl2",
		Table2:   "test_tbl1",
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_TailTable(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/table/tail") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "GET" {
			log.Fatalln("Method must be GET")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `[{"time":1585732845,"c0":"ccccc","c1":"22222,44444"},{"time":1585732845,"c0":"ccccc","c1":"22222,44444"},{"time":1585732845,"c0":"ccccc","c1":"22222,44444"}]`)
		if err != nil {
			return
		}
	}))
	defer ts.Close()

	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	got, err := client.TailTable(context.Background(), "testdb", "test_tbl1", 10)
	want := `[{"time":1585732845,"c0":"ccccc","c1":"22222,44444"},{"time":1585732845,"c0":"ccccc","c1":"22222,44444"},{"time":1585732845,"c0":"ccccc","c1":"22222,44444"}]`

	if got != want {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_UpdateSchema(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/table/update-schema") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		if !strings.Contains(r.Header.Get("content-type"), "application/x-www-form-urlencoded") {
			log.Fatalf("The content-type does not match the expectation: got: %s", r.Header.Get("content-type"))
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "POST" {
			log.Fatalln("Method must be POST")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `{"database": "testdb","table": "test_tbl1","type": "log"}`)
		if err != nil {
			return
		}
	}))
	defer ts.Close()

	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	got, err := client.UpdateSchema(context.Background(), "testdb", "test_tbl1", "[[\"c1\",\"string\",\"c1\"],[\"c0\",\"string\",\"c0\"]]")
	want := &TableInfo{
		Database: "testdb",
		Table:    "test_tbl1",
		Type:     "log",
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

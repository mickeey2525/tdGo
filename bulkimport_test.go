package tdGo

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestClient_BulkImportCommit(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/bulk_import/commit/test") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "POST" {
			log.Fatalln("Method must be POST")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `{"name": "test_bulk_import","bulk_import": "test_bulk_import"}`)
		if err != nil {
			return
		}
	}))
	defer ts.Close()

	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	got, err := client.BulkImportCommit(context.Background(), "test_bulk_import")
	if err != nil {
		log.Fatalln(err)
	}
	want := &BulkImport{
		Name:       "test_bulk_import",
		BulkImport: "test_bulk_import",
	}

	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_BulkImportCreate(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/bulk_import/create/test_bulk_import/test_db/table_a1") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "POST" {
			log.Fatalln("Method must be POST")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `{"name": "test_bulk_import","bulk_import": "test_bulk_import"}`)
		if err != nil {
			return
		}
	}))
	defer ts.Close()
	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	got, err := client.BulkImportCreate(context.Background(), "test_bulk_import", "test_db", "table_a1")
	if err != nil {
		log.Fatalln(err)
	}
	want := &BulkImport{
		Name:       "test_bulk_import",
		BulkImport: "test_bulk_import",
	}

	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}

}

func TestClient_BulkImportDelete(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/bulk_import/delete/test_bulk_import") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "POST" {
			log.Fatalln("Method must be POST")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `{"name": "test_bulk_import","bulk_import": "test_bulk_import"}`)
		if err != nil {
			return
		}
	}))
	defer ts.Close()
	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	got, err := client.BulkImportDelete(context.Background(), "test_bulk_import")
	if err != nil {
		log.Fatalln(err)
	}
	want := &BulkImport{
		Name:       "test_bulk_import",
		BulkImport: "test_bulk_import",
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_BulkImportFreeze(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/bulk_import/freeze/test_bulk_import") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "POST" {
			log.Fatalln("Method must be POST")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `{"name": "test_bulk_import","bulk_import": "test_bulk_import"}`)
		if err != nil {
			return
		}
	}))
	defer ts.Close()
	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	got, err := client.BulkImportFreeze(context.Background(), "test_bulk_import")
	if err != nil {
		log.Fatalln(err)
	}
	want := &BulkImport{
		Name:       "test_bulk_import",
		BulkImport: "test_bulk_import",
	}

	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_BulkImportList(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/bulk_import/list") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "GET" {
			log.Fatalln("Method must be POST")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `
{
    "bulk_imports": [
        {
            "name": "bulk_import_test_session",
            "valid_records": 5,
            "error_records": 0,
            "valid_parts": 1,
            "error_parts": 0,
            "status": "committed",
            "upload_frozen": true,
            "database": "tdtests",
            "table": "bulk_import_test",
            "job_id": "50696647"
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
	got, err := client.BulkImportList(context.Background())
	if err != nil {
		log.Fatalln(err)
	}
	want := &BulkImportList{
		[]BulkImportDetail{
			{Name: "bulk_import_test_session", ValidRecords: 5, ErrorRecords: 0, ValidParts: 1, ErrorParts: 0, Status: "committed", UploadFrozen: true, Database: "tdtests", Table: "bulk_import_test", JobId: "50696647"},
		},
	}

	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v\n, but got %+v", want, got)
	}
}

func TestClient_BulkImportPerform(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/bulk_import/perform/test_bulk_import") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "POST" {
			log.Fatalln("Method must be POST")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `{"name": "test_bulk_import","bulk_import": "test_bulk_import", "job_id": 50707565}`)
		if err != nil {
			return
		}
	}))
	defer ts.Close()
	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	got, err := client.BulkImportPerform(context.Background(), "test_bulk_import")
	if err != nil {
		log.Fatalln(err)
	}
	want := &BulkImportJob{
		BulkImport: "test_bulk_import",
		Name:       "test_bulk_import",
		JobId:      50707565,
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v\n, but got %+v", want, got)
	}
}

func TestClient_BulkImportShow(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/bulk_import/show/test_bulk_import") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "GET" {
			log.Fatalln("Method must be POST")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `{
            "name": "bulk_import_test_session",
            "valid_records": 5,
            "error_records": 0,
            "valid_parts": 1,
            "error_parts": 0,
            "status": "committed",
            "upload_frozen": true,
            "database": "tdtests",
            "table": "bulk_import_test",
            "job_id": "50696647"
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
	got, err := client.BulkImportShow(context.Background(), "test_bulk_import")
	if err != nil {
		log.Fatalln(err)
	}
	want := &BulkImportDetail{
		Name: "bulk_import_test_session", ValidRecords: 5, ErrorRecords: 0, ValidParts: 1, ErrorParts: 0, Status: "committed", UploadFrozen: true, Database: "tdtests", Table: "bulk_import_test", JobId: "50696647",
	}

	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v\n, but got %+v", want, got)
	}
}

func TestClient_BulkImportUnFreeze(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/bulk_import/unfreeze/test_bulk_import") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "POST" {
			log.Fatalln("Method must be POST")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `{"name": "test_bulk_import","bulk_import": "test_bulk_import"}`)
		if err != nil {
			return
		}
	}))
	defer ts.Close()
	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	got, err := client.BulkImportUnFreeze(context.Background(), "test_bulk_import")
	if err != nil {
		log.Fatalln(err)
	}
	want := &BulkImport{
		Name:       "test_bulk_import",
		BulkImport: "test_bulk_import",
	}

	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_BulkImportUpload(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/bulk_import/upload_part/test_bulk_import/test_bulk_import") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "PUT" {
			log.Fatalln("Method must be POST")
		}
		fmt.Println(r.Header)
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `{"name": "test_bulk_import","bulk_import": "test_bulk_import"}`)
		if err != nil {
			return
		}
	}))
	defer ts.Close()
	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	data, err := os.ReadFile("./testdata/test.msgpack.gz")
	reader := bytes.NewReader(data)
	got, err := client.BulkImportUpload(context.Background(), "test_bulk_import", "test_bulk_import", reader)
	if err != nil {
		log.Fatalln(err)
	}

	want := &BulkImport{
		Name:       "test_bulk_import",
		BulkImport: "test_bulk_import",
	}

	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

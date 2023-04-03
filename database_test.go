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

func TestClient_CreateDB(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/database/create") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "POST" {
			log.Fatalln("Method must be POST")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `{"database": "test"}`)
		if err != nil {
			return
		}
	}))
	defer ts.Close()

	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	got, err := client.CreateDB(context.Background(), "test")
	want := &DBName{
		Database: "test",
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_DeleteDB(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/database/delete") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "POST" {
			log.Fatalln("Method must be POST")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `{"database": "test"}`)
		if err != nil {
			return
		}
	}))
	defer ts.Close()

	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	got, err := client.DeleteDB(context.Background(), "test")
	want := &DBName{
		Database: "test",
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_GetDBList(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		fmt.Printf("The Http methos is %s\n", r.Method)
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `
{
  "databases": [
    {
      "name": "test",
      "created_at": "2020-06-11 10:25:10 UTC",
      "updated_at": "2020-06-11 10:25:10 UTC",
      "count": 8,
      "organization": null,
      "permission": "administrator",
      "delete_protected": false
    },
    {
      "name": "test2",
      "created_at": "2019-07-02 02:44:56 UTC",
      "updated_at": "2019-07-02 02:44:56 UTC",
      "count": 1314649249,
      "organization": null,
      "permission": "administrator",
      "delete_protected": true
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
	got, err := client.GetDBList(context.Background())
	if err != nil {
		log.Fatalln(err)
	}
	want := &Databases{[]Database{
		{Name: "test", CreatedAt: "2020-06-11 10:25:10 UTC", UpdatedAt: "2020-06-11 10:25:10 UTC", Count: 8, Organization: nil, Permission: "administrator", DeleteProtected: false},
		{Name: "test2", CreatedAt: "2019-07-02 02:44:56 UTC", UpdatedAt: "2019-07-02 02:44:56 UTC", Count: 1314649249, Organization: nil, Permission: "administrator", DeleteProtected: true},
	}}

	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_ShowDB(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		fmt.Printf("The Http methos is %s\n", r.Method)
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `
    {
      "name": "test",
      "created_at": "2020-06-11 10:25:10 UTC",
      "updated_at": "2020-06-11 10:25:10 UTC",
      "count": 8,
      "organization": null,
      "permission": "administrator",
      "delete_protected": false
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
	got, err := client.ShowDB(context.Background(), "test")
	if err != nil {
		log.Fatalln(err)
	}
	want := &Database{
		Name: "test", CreatedAt: "2020-06-11 10:25:10 UTC", UpdatedAt: "2020-06-11 10:25:10 UTC", Count: 8, Organization: nil, Permission: "administrator", DeleteProtected: false,
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

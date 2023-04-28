package tdGo

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestShowColumnPermission(t *testing.T) {
	testPolicyID := 1
	testColumnPermission := ColumnPermission{
		Masking: "hash",
		Tags:    []string{"pii"},
		Except:  false,
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		json.NewEncoder(w).Encode(testColumnPermission)
	}))
	defer ts.Close()

	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}

	columnPermission, err := client.ShowColumnPermission(context.Background(), testPolicyID)
	if err != nil {
		log.Fatalln(err)
	}

	want := &testColumnPermission
	if !reflect.DeepEqual(want, columnPermission) {
		t.Errorf("want %+v, but got %+v", want, columnPermission)
	}
}

func TestUpdateColumnPermission(t *testing.T) {
	testPolicyID := 1
	testColumnPermission := ColumnPermission{
		Masking: "hash",
		Tags:    []string{"pii"},
		Except:  false,
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		json.NewEncoder(w).Encode(testColumnPermission)
	}))
	defer ts.Close()

	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}

	updatedColumnPermission, err := client.UpdateColumnPermission(context.Background(), testPolicyID, testColumnPermission)
	if err != nil {
		log.Fatalln(err)
	}

	want := &testColumnPermission
	if !reflect.DeepEqual(want, updatedColumnPermission) {
		t.Errorf("want %+v, but got %+v", want, updatedColumnPermission)
	}
}

func TestShowColumnPermission_Error(t *testing.T) {
	testPolicyID := 1

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Write([]byte(`{"error": "Internal Server Error"}`))
	}))
	defer ts.Close()

	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		t.Fatal(err)
	}

	columnPermission, err := client.ShowColumnPermission(context.Background(), testPolicyID)
	assert.Error(t, err)
	assert.Nil(t, columnPermission)
}

func TestUpdateColumnPermission_Error(t *testing.T) {
	testPolicyID := 1
	testColumnPermission := ColumnPermission{
		Masking: "hash",
		Tags:    []string{"pii"},
		Except:  false,
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Write([]byte(`{"error": "Bad Request"}`))
	}))
	defer ts.Close()

	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		t.Fatal(err)
	}

	updatedColumnPermission, err := client.UpdateColumnPermission(context.Background(), testPolicyID, testColumnPermission)
	assert.Error(t, err)
	assert.Nil(t, updatedColumnPermission)
}

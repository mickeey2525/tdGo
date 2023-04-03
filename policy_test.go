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

func TestClient_AttachUserPolicy(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/access_control/users/123/policies/67") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "POST" {
			log.Fatalln("Method must be POST")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `
	{
		"id": 67,
		"account_id": 123,
		"name": "some_policy",
		"description": "written about the policy",
		"user_count": 3
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
	got, err := client.AttachUserPolicy(context.Background(), 123, 67)
	if err != nil {
		log.Fatalln(err)
	}
	want := &Policy{
		AccountId: 123, Description: "written about the policy", Id: 67, Name: "some_policy", UserCount: 3,
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_CreatePolicy(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/access_control/policies") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "POST" {
			log.Fatalln("Method must be POST")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `
	{
		"id": 67,
		"account_id": 123,
		"name": "some_policy",
		"description": "written about the policy",
		"user_count": 3
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
	policyOption := PolicyOption{}
	policyOption.Policy.Name = "some_policy"
	policyOption.Policy.Description = "written about the policy"
	got, err := client.CreatePolicy(context.Background(), policyOption)
	if err != nil {
		log.Fatalln(err)
	}
	want := &Policy{
		AccountId: 123, Description: "written about the policy", Id: 67, Name: "some_policy", UserCount: 3,
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_DeletePolicy(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/access_control/policies/67") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "DELETE" {
			log.Fatalln("Method must be DELETE")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `
	{
		"id": 67,
		"account_id": 123,
		"name": "some_policy",
		"description": "written about the policy",
		"user_count": 3
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

	got, err := client.DeletePolicy(context.Background(), 67)
	if err != nil {
		log.Fatalln(err)
	}
	want := &Policy{
		AccountId: 123, Description: "written about the policy", Id: 67, Name: "some_policy", UserCount: 3,
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_DetachUserPolicy(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/access_control/users/123/policies/67") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "DELETE" {
			log.Fatalln("Method must be DELETE")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `
	{
		"id": 67,
		"account_id": 123,
		"name": "some_policy",
		"description": "written about the policy",
		"user_count": 3
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

	got, err := client.DetachUserPolicy(context.Background(), 123, 67)
	if err != nil {
		log.Fatalln(err)
	}
	want := &Policy{
		AccountId: 123, Description: "written about the policy", Id: 67, Name: "some_policy", UserCount: 3,
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_GetPolicy(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/access_control/policies/67") {
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
		"id": 67,
		"account_id": 123,
		"name": "some_policy",
		"description": "written about the policy",
		"user_count": 3
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
	got, err := client.GetPolicy(context.Background(), 67)
	if err != nil {
		log.Fatalln(err)
	}
	want := &Policy{
		AccountId: 123, Description: "written about the policy", Id: 67, Name: "some_policy", UserCount: 3,
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_GetPolicyList(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/access_control/policies") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "GET" {
			log.Fatalln("Method must be POST")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `
[
	{
		"id": 67,
		"account_id": 123,
		"name": "some_policy",
		"description": "written about the policy",
		"user_count": 3
	}
]`)
		if err != nil {
			return
		}
	}))
	defer ts.Close()

	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	got, err := client.GetPolicyList(context.Background())
	if err != nil {
		log.Fatalln(err)
	}
	want := &PolicyList{
		{AccountId: 123, Description: "written about the policy", Id: 67, Name: "some_policy", UserCount: 3},
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_GetUserPolicy(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/access_control/users/12345/policies") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "GET" {
			log.Fatalln("Method must be GET")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `
[	
{
		"id": 67,
		"account_id": 123,
		"name": "some_policy",
		"description": "written about the policy",
		"user_count": 3
	}
]`)
		if err != nil {
			return
		}
	}))
	defer ts.Close()

	client, err := NewClient("aaa", ts.URL)
	if err != nil {
		log.Fatalln(err)
	}
	got, err := client.GetUserPolicy(context.Background(), 12345)
	if err != nil {
		log.Fatalln(err)
	}
	want := &PolicyList{
		{AccountId: 123, Description: "written about the policy", Id: 67, Name: "some_policy", UserCount: 3},
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_UpdatePolicy(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/access_control/policies") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "PATCH" {
			log.Fatalln("Method must be PATCH")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `
	{
		"id": 67,
		"account_id": 123,
		"name": "some_policy",
		"description": "newly updated policy",
		"user_count": 3
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
	policyOption := PolicyOption{}
	policyOption.Policy.Name = "some_policy"
	policyOption.Policy.Description = "newly updated policy"
	got, err := client.UpdatePolicy(context.Background(), 67, policyOption)
	if err != nil {
		log.Fatalln(err)
	}
	want := &Policy{
		AccountId: 123, Description: "newly updated policy", Id: 67, Name: "some_policy", UserCount: 3,
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

func TestClient_UpdateUserPolicy(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("The url is %s\n", r.URL.String())
		if !strings.Contains(r.URL.String(), "/v3/access_control/users/12345/policies") {
			log.Fatalf("The path does not match the expectation: got: %s", r.URL.String())
		}
		fmt.Printf("The Http methos is %s\n", r.Method)
		if r.Method != "PATCH" {
			log.Fatalln("Method must be PATCH")
		}
		fmt.Println()
		w.Header().Set("content-type", "application/json; charset=utf-8")
		_, err := io.WriteString(w, `
	{
		"id": 67,
		"account_id": 123,
		"name": "some_policy",
		"description": "newly updated policy",
		"user_count": 3
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
	policyIds := PolicyIdSet{
		PolicyIds: []string{"1", "2"},
	}
	got, err := client.UpdateUserPolicy(context.Background(), 12345, policyIds)
	if err != nil {
		log.Fatalln(err)
	}
	want := &Policy{
		AccountId: 123, Description: "newly updated policy", Id: 67, Name: "some_policy", UserCount: 3,
	}
	if !reflect.DeepEqual(want, got) {
		t.Errorf("want %+v, but got %+v", want, got)
	}
}

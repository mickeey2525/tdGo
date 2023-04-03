package tdGo

import (
	"context"
	"fmt"
)

type Databases struct {
	Databases []Database `json:"databases"`
}

type Database struct {
	Name            string      `json:"name"`
	CreatedAt       string      `json:"created_at"`
	UpdatedAt       string      `json:"updated_at"`
	Count           int         `json:"count"`
	Organization    interface{} `json:"organization"`
	Permission      string      `json:"permission"`
	DeleteProtected bool        `json:"delete_protected"`
}

func (c *Client) GetDBList(ctx context.Context) (*Databases, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&Databases{}).Get(c.baseURL.String() + "/v3/database/list")
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}

	ok := checkStatus(resp)
	if ok != nil {
		return nil, err
	}

	return resp.Result().(*Databases), nil
}

func (c *Client) ShowDB(ctx context.Context, dbName string) (*Database, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&Database{}).Get(c.baseURL.String() + fmt.Sprintf("/v3/database/show/%s", dbName))
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}

	ok := checkStatus(resp)
	if ok != nil {
		return nil, err
	}

	return resp.Result().(*Database), nil
}

type DBName struct {
	Database string `json:"database"`
}

func (c *Client) CreateDB(ctx context.Context, dbName string) (*DBName, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&DBName{}).Post(c.baseURL.String() + fmt.Sprintf("/v3/database/create/%s", dbName))
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}

	ok := checkStatus(resp)
	if ok != nil {
		return nil, err
	}
	return resp.Result().(*DBName), nil
}

func (c *Client) DeleteDB(ctx context.Context, dbName string) (*DBName, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&DBName{}).Post(c.baseURL.String() + fmt.Sprintf("/v3/database/delete/%s", dbName))
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}

	ok := checkStatus(resp)
	if ok != nil {
		return nil, err
	}
	return resp.Result().(*DBName), nil
}

package tdGo

import (
	"context"
	"fmt"
)

type Result struct {
	Name         string      `json:"name"`
	Url          string      `json:"url"`
	Organization interface{} `json:"organization"`
}

type Results struct {
	Results []Result `json:"results"`
}

func (c *Client) GetConnections(ctx context.Context) (*Results, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&Results{}).Get(c.baseURL.String() + "/v3/result/list")
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return nil, ok
	}
	return resp.Result().(*Results), nil
}

type ConnectionResult struct {
	Name   string `json:"name"`
	Result string `json:"result"`
}

func (c *Client) CreateConnections(ctx context.Context, connectionName string, connectionSettings map[string]string, connectionUrl string) (*ConnectionResult, error) {
	c.setHeaders(c.httpClient)
	param := make(map[string]string)
	if connectionSettings != nil {
		for k, v := range connectionSettings {
			param[k] = v
		}
	}
	param["url"] = connectionUrl
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&ConnectionResult{}).SetBody(param).Post(c.baseURL.String() + fmt.Sprintf("/v3/result/create/%s", connectionName))
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return nil, ok
	}
	return resp.Result().(*ConnectionResult), nil
}

func (c *Client) DeleteConnection(ctx context.Context, connectionName string) (*ConnectionResult, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&ConnectionResult{}).Post(c.baseURL.String() + fmt.Sprintf("/v3/result/delete/%s", connectionName))
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return nil, ok
	}
	return resp.Result().(*ConnectionResult), nil
}

type ConnectionId struct {
	ID int `json:"id"`
}

func (c *Client) GetConnectionIdWithName(ctx context.Context, connectionName string) (*ConnectionId, error) {
	c.setHeaders(c.httpClient)
	c.httpClient.SetQueryParam("name", connectionName)
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&ConnectionId{}).Get(c.baseURL.String() + "/v3/connections/lookup")
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return nil, ok
	}
	return resp.Result().(*ConnectionId), nil
}

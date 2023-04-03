package tdGo

import (
	"context"
	"fmt"
)

type Policy struct {
	AccountId   int    `json:"account_id"`
	Description string `json:"description"`
	Id          int    `json:"id"`
	Name        string `json:"name"`
	UserCount   int    `json:"user_count"`
}

type PolicyList []Policy

func (c *Client) GetPolicyList(ctx context.Context) (*PolicyList, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&PolicyList{}).Get(c.baseURL.String() + "/v3/access_control/policies")
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
	return resp.Result().(*PolicyList), nil
}

type PolicyOption struct {
	Policy struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	} `json:"policy"`
}

func (c *Client) CreatePolicy(ctx context.Context, policy PolicyOption) (*Policy, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&Policy{}).SetBody(policy).Post(c.baseURL.String() + "/v3/access_control/policies")
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
	return resp.Result().(*Policy), nil
}

func (c *Client) GetPolicy(ctx context.Context, policyId int) (*Policy, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&Policy{}).Get(c.baseURL.String() + fmt.Sprintf("/v3/access_control/policies/%d", policyId))
	if err != nil {
		return nil, err
	}
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
	return resp.Result().(*Policy), nil
}

func (c *Client) UpdatePolicy(ctx context.Context, policyId int, policy PolicyOption) (*Policy, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&Policy{}).SetBody(policy).Patch(c.baseURL.String() + fmt.Sprintf("/v3/access_control/policies/%d", policyId))
	if err != nil {
		return nil, err
	}
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
	return resp.Result().(*Policy), nil
}

func (c *Client) DeletePolicy(ctx context.Context, policyId int) (*Policy, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&Policy{}).Delete(c.baseURL.String() + fmt.Sprintf("/v3/access_control/policies/%d", policyId))
	if err != nil {
		return nil, err
	}
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
	return resp.Result().(*Policy), nil
}

func (c *Client) GetUserPolicy(ctx context.Context, userId int) (*PolicyList, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&PolicyList{}).Get(c.baseURL.String() + fmt.Sprintf("/v3/access_control/users/%d/policies", userId))
	if err != nil {
		return nil, err
	}
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
	return resp.Result().(*PolicyList), nil
}

type PolicyIdSet struct {
	PolicyIds []string `json:"policy_ids"`
}

func (c *Client) UpdateUserPolicy(ctx context.Context, userId int, policySet PolicyIdSet) (*Policy, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&Policy{}).SetBody(policySet).Patch(c.baseURL.String() + fmt.Sprintf("/v3/access_control/users/%d/policies", userId))
	if err != nil {
		return nil, err
	}
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
	return resp.Result().(*Policy), nil
}

func (c *Client) AttachUserPolicy(ctx context.Context, userId int, policyId int) (*Policy, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&Policy{}).Post(c.baseURL.String() + fmt.Sprintf("/v3/access_control/users/%d/policies/%d", userId, policyId))
	if err != nil {
		return nil, err
	}
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
	return resp.Result().(*Policy), nil
}

func (c *Client) DetachUserPolicy(ctx context.Context, userId int, policyId int) (*Policy, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&Policy{}).Delete(c.baseURL.String() + fmt.Sprintf("/v3/access_control/users/%d/policies/%d", userId, policyId))
	if err != nil {
		return nil, err
	}
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
	return resp.Result().(*Policy), nil
}

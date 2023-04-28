package tdGo

import (
	"context"
	"fmt"
)

func (c *Client) GetUserPermissions(ctx context.Context) (string, error) {

	resp, err := c.httpClient.R().SetContext(ctx).Get(c.baseURL.String() + "/v3/access_control/users")
	if err != nil {
		return "", err
	}
	if resp.IsError() {
		return "", fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return "", ok
	}
	return resp.String(), nil
}

// GetUserPermissionsWithId provides specific user's permissions
func (c *Client) GetUserPermissionsWithId(ctx context.Context, userId int) (string, error) {

	resp, err := c.httpClient.R().SetContext(ctx).Get(c.baseURL.String() + fmt.Sprintf("/v3/access_control/users/%d", userId))
	if err != nil {
		return "", err
	}
	if resp.IsError() {
		return "", fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return "", ok
	}
	return resp.String(), nil
}

// UpdateUserPermissions updates specific users permissions
// For permission schema, see this link https://api-docs.treasuredata.com/pages/td-api/tag/Access-Control-Users/#tag/Access-Control-Users/operation/updateAccessControlUserPermissions
func (c *Client) UpdateUserPermissions(ctx context.Context, userId int, permissions string) (string, error) {

	resp, err := c.httpClient.R().SetContext(ctx).SetBody(permissions).Patch(c.baseURL.String() + fmt.Sprintf("/v3/access_control/users/%d/permission", userId))
	if err != nil {
		return "", err
	}
	if resp.IsError() {
		return "", fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return "", ok
	}
	return resp.String(), nil
}

func (c *Client) GetPolicyUsers(ctx context.Context, policyId int) (string, error) {

	resp, err := c.httpClient.R().SetContext(ctx).Patch(c.baseURL.String() + fmt.Sprintf("/v3/access_control/policies/%d/users", policyId))
	if err != nil {
		return "", err
	}
	if resp.IsError() {
		return "", fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return "", ok
	}
	return resp.String(), nil
}

type PolicyUsers struct {
	UserIds []int `json:"user_ids"`
}

func (c *Client) UpdatePolicyUsers(ctx context.Context, policyId int, users PolicyUsers) (string, error) {

	resp, err := c.httpClient.R().SetContext(ctx).SetBody(users).Patch(c.baseURL.String() + fmt.Sprintf("/v3/access_control/policies/%d/users", policyId))
	if err != nil {
		return "", err
	}
	if resp.IsError() {
		return "", fmt.Errorf("API error: %s: %s", resp.Status(), string(resp.Body()))
	}
	ok := checkStatus(resp)
	if ok != nil {
		return "", ok
	}
	return resp.String(), nil
}

package tdGo

import (
	"context"
	"fmt"
)

type ColumnPermission struct {
	Masking string   `json:"masking,omitempty"`
	Tags    []string `json:"tags"`
	Except  bool     `json:"except,omitempty"`
}

func (c *Client) ShowColumnPermission(ctx context.Context, policyId int) (*ColumnPermission, error) {

	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&ColumnPermission{}).Get(c.baseURL.String() + fmt.Sprintf("/v3/access_control/policies/%d/column_permissions", policyId))
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
	return resp.Result().(*ColumnPermission), nil
}

func (c *Client) UpdateColumnPermission(ctx context.Context, policyId int, permission ColumnPermission) (*ColumnPermission, error) {

	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&ColumnPermission{}).SetBody(permission).Patch(c.baseURL.String() + fmt.Sprintf("/v3/access_control/policies/%d/column_permissions", policyId))
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
	return resp.Result().(*ColumnPermission), nil
}

func (c *Client) ShowPolicyPermissions(ctx context.Context, policyId int) (string, error) {

	resp, err := c.httpClient.R().SetContext(ctx).Get(c.baseURL.String() + fmt.Sprintf("/v3/access_control/policies/%d/permissions", policyId))
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

// UpdatePolicyPermissions updates specified policy's permissions.
// For the Permissions body, please see https://api-docs.treasuredata.com/pages/td-api/tag/Access-Control-Permissions/#tag/Access-Control-Permissions/operation/updatePermissionByPolicyId
func (c *Client) UpdatePolicyPermissions(ctx context.Context, policyId int, permissions string) (string, error) {

	resp, err := c.httpClient.R().SetContext(ctx).SetBody(permissions).Patch(c.baseURL.String() + fmt.Sprintf("/v3/access_control/policies/%d/permissions", policyId))
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

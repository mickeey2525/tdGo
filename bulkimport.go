package tdGo

import (
	"context"
	"fmt"
)

type BulkImport struct {
	Name       string `json:"name"`
	BulkImport string `json:"bulk_import"`
}

func (c *Client) BulkImportCreate(ctx context.Context, name, db, tbl string) (*BulkImport, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetResult(&BulkImport{}).SetContext(ctx).Post(c.baseURL.String() + fmt.Sprintf("/v3/bulk_import/create/%s/%s/%s", name, db, tbl))
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

	return resp.Result().(*BulkImport), nil
}

func (c *Client) BulkImportUpload(ctx context.Context, name, tbl string, files []byte) (*BulkImport, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetResult(&BulkImport{}).SetContext(ctx).SetBody(files).Put(c.baseURL.String() + fmt.Sprintf("/v3/bulk_import/upload_part/%s/%s", tbl, name))
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

	return resp.Result().(*BulkImport), nil
}

func (c *Client) BulkImportCommit(ctx context.Context, name string) (*BulkImport, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetResult(&BulkImport{}).SetContext(ctx).Post(c.baseURL.String() + fmt.Sprintf("/v3/bulk_import/commit/%s", name))
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

	return resp.Result().(*BulkImport), nil
}

func (c *Client) BulkImportDelete(ctx context.Context, name string) (*BulkImport, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetResult(&BulkImport{}).SetContext(ctx).Post(c.baseURL.String() + fmt.Sprintf("/v3/bulk_import/delete/%s", name))
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

	return resp.Result().(*BulkImport), nil
}

func (c *Client) BulkImportFreeze(ctx context.Context, name string) (*BulkImport, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetResult(&BulkImport{}).SetContext(ctx).Post(c.baseURL.String() + fmt.Sprintf("/v3/bulk_import/freeze/%s", name))
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

	return resp.Result().(*BulkImport), nil
}

func (c *Client) BulkImportUnFreeze(ctx context.Context, name string) (*BulkImport, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetResult(&BulkImport{}).SetContext(ctx).Post(c.baseURL.String() + fmt.Sprintf("/v3/bulk_import/unfreeze/%s", name))
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

	return resp.Result().(*BulkImport), nil
}

type BulkImportDetail struct {
	JobId        string `json:"job_id"`
	Database     string `json:"database"`
	ErrorParts   int    `json:"error_parts"`
	ErrorRecords int    `json:"error_records"`
	Name         string `json:"name"`
	Status       string `json:"status"`
	Table        string `json:"table"`
	UploadFrozen bool   `json:"upload_frozen"`
	ValidParts   int    `json:"valid_parts"`
	ValidRecords int    `json:"valid_records"`
}

type BulkImportList struct {
	BulkImports []BulkImportDetail `json:"bulk_imports"`
}

func (c *Client) BulkImportList(ctx context.Context) (*BulkImportList, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetResult(&BulkImportList{}).SetContext(ctx).Get(c.baseURL.String() + "/v3/bulk_import/list")
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

	return resp.Result().(*BulkImportList), nil
}

type BulkImportJob struct {
	Name       string `json:"name"`
	BulkImport string `json:"bulk_import"`
	JobId      int    `json:"job_id"`
}

func (c *Client) BulkImportPerform(ctx context.Context, name string) (*BulkImportJob, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetResult(&BulkImportJob{}).SetContext(ctx).Post(c.baseURL.String() + fmt.Sprintf("/v3/bulk_import/perform/%s", name))
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

	return resp.Result().(*BulkImportJob), nil
}

func (c *Client) BulkImportShow(ctx context.Context, name string) (*BulkImportDetail, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetResult(&BulkImportDetail{}).SetContext(ctx).Get(c.baseURL.String() + fmt.Sprintf("/v3/bulk_import/show/%s", name))
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

	return resp.Result().(*BulkImportDetail), nil
}

package tdGo

import (
	"context"
	"fmt"
)

// BulkImport is a struct for bulk import
type BulkImport struct {
	// Name is the name of the bulk import
	Name string `json:"name"`
	// BulkImport is the bulk import
	BulkImport string `json:"bulk_import"`
}

// BulkImportCreate creates a bulk import
func (c *Client) BulkImportCreate(ctx context.Context, name, db, tbl string) (*BulkImport, error) {
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

// BulkImportUpload uploads files to bulkimport sessions
func (c *Client) BulkImportUpload(ctx context.Context, name, tbl string, files []byte) (*BulkImport, error) {
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

// BulkImportCommit commits a bulk import
func (c *Client) BulkImportCommit(ctx context.Context, name string) (*BulkImport, error) {
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

// BulkImportDelete deletes a bulk import session
func (c *Client) BulkImportDelete(ctx context.Context, name string) (*BulkImport, error) {
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

// BulkImportFreeze freezes a bulk import session
func (c *Client) BulkImportFreeze(ctx context.Context, name string) (*BulkImport, error) {

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

// BulkImportUnFreeze unfreezes a bulk import session
func (c *Client) BulkImportUnFreeze(ctx context.Context, name string) (*BulkImport, error) {
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

// BulkImportDetail is a struct for bulk import details
type BulkImportDetail struct {
	// JobId is the job id of the bulk import
	JobId string `json:"job_id"`
	// Database is the database of the bulk import
	Database string `json:"database"`
	// ErrorParts is the number of error parts
	ErrorParts int `json:"error_parts"`
	// ErrorRecords is the number of error records
	ErrorRecords int `json:"error_records"`
	// Name is the name of the bulk import
	Name string `json:"name"`
	// Status is the status of the bulk import
	Status string `json:"status"`
	// Table is the table of the bulk import
	Table string `json:"table"`
	// UploadFrozen is the upload frozen status of the bulk import
	UploadFrozen bool `json:"upload_frozen"`
	// ValidParts is the number of valid parts
	ValidParts int `json:"valid_parts"`
	// ValidRecords is the number of valid records
	ValidRecords int `json:"valid_records"`
}

// BulkImportList is a struct for bulk import list
type BulkImportList struct {
	BulkImports []BulkImportDetail `json:"bulk_imports"`
}

// BulkImportList lists all bulk imports
func (c *Client) BulkImportList(ctx context.Context) (*BulkImportList, error) {
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

// BulkImportJob is a struct for bulk import job
type BulkImportJob struct {
	Name       string `json:"name"`
	BulkImport string `json:"bulk_import"`
	JobId      int    `json:"job_id"`
}

// BulkImportPerform performs a bulk import
func (c *Client) BulkImportPerform(ctx context.Context, name string) (*BulkImportJob, error) {
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

// BulkImportShow shows a bulk import session
func (c *Client) BulkImportShow(ctx context.Context, name string) (*BulkImportDetail, error) {
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

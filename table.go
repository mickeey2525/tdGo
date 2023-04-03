package tdGo

import (
	"context"
	"fmt"
	"strconv"
)

// Table represents one table information
type Table struct {
	Id                   int         `json:"id"`
	Name                 string      `json:"name"`
	EstimatedStorageSize int         `json:"estimated_storage_size"`
	CounterUpdatedAt     string      `json:"counter_updated_at"`
	LastLogTimestamp     string      `json:"last_log_timestamp"`
	DeleteProtected      bool        `json:"delete_protected"`
	CreatedAt            string      `json:"created_at"`
	UpdatedAt            string      `json:"updated_at"`
	Type                 string      `json:"type"`
	IncludeV             bool        `json:"include_v"`
	Count                int         `json:"count"`
	Schema               string      `json:"schema"`
	ExpireDays           interface{} `json:"expire_days"`
}

// Tables shows all tables of a database
type Tables struct {
	Database string  `json:"database"`
	Tables   []Table `json:"tables"`
}

// GetTablesList retrieves all tables info from td-api in Tables struct format
// see https://api-docs.treasuredata.com/pages/td-api/tag/Tables/#tag/Tables/operation/getTablesByDatabaseName
func (c *Client) GetTablesList(ctx context.Context, dbName string) (*Tables, error) {
	c.setHeaders(c.httpClient)

	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&Tables{}).Get(c.baseURL.String() + fmt.Sprintf("/v3/table/list/%s", dbName))
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
	return resp.Result().(*Tables), nil
}

// GetTable retrieves a table info from td-api in Table struct format
// see https://api-docs.treasuredata.com/pages/td-api/tag/Tables/#tag/Tables/operation/getTableByDatabaseNameAndTableName
func (c *Client) GetTable(ctx context.Context, dbName string, tblName string) (*Table, error) {
	c.setHeaders(c.httpClient)

	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&Table{}).Get(c.baseURL.String() + fmt.Sprintf("/v3/table/show/%s/%s", dbName, tblName))
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
	return resp.Result().(*Table), nil
}

// TableInfo represents API response when you create/delete table
type TableInfo struct {
	// Database means database name of that table belongs to
	Database string `json:"database"`
	// Table means that table name
	Table string `json:"table"`
	// Type does not mean that much. You will see only log type
	Type string `json:"type"`
}

// DeleteTable deletes a table of a database and if succeeds, returns TableInfo pointer.
func (c *Client) DeleteTable(ctx context.Context, dbName string, tblName string) (*TableInfo, error) {
	c.setHeaders(c.httpClient)

	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&TableInfo{}).Post(c.baseURL.String() + fmt.Sprintf("/v3/table/delete/%s/%s", dbName, tblName))
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
	return resp.Result().(*TableInfo), nil
}

// CreateTable creates a table of a database and if succeeds, returns TableInfo pointer.
func (c *Client) CreateTable(ctx context.Context, dbName string, tblName string) (*TableInfo, error) {
	c.setHeaders(c.httpClient)

	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&TableInfo{}).Post(c.baseURL.String() + fmt.Sprintf("/v3/table/create/%s/%s", dbName, tblName))
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
	return resp.Result().(*TableInfo), nil
}

// UpdateSchema updates a table's schema.
// Schema format should be like [["sep_len", "long", "sep_len"], ["sep_wid", "long", "sep_wid"]]
// ["sep_len", "long", "sep_len"] each means column_name, data_type, query_as(alias) name
func (c *Client) UpdateSchema(ctx context.Context, dbName string, tblName string, schema string) (*TableInfo, error) {
	c.setHeaders(c.httpClient)
	body := map[string]string{"schema": schema}
	c.httpClient.SetHeader("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.httpClient.R().SetContext(ctx).SetFormData(body).SetResult(&TableInfo{}).Post(c.baseURL.String() + fmt.Sprintf("/v3/table/update-schema/%s/%s", dbName, tblName))
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
	return resp.Result().(*TableInfo), nil
}

// RenameTable changes table name.
func (c *Client) RenameTable(ctx context.Context, dbName string, currentTblName string, newTblName string) (*TableInfo, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(TableInfo{}).Post(c.baseURL.String() + fmt.Sprintf("/v3/table/rename/%s/%s/%s", dbName, currentTblName, newTblName))
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
	return resp.Result().(*TableInfo), nil
}

type SwapInfo struct {
	// Database means database name of that table belongs to
	Database string `json:"database"`
	// Table1 means that table name of the second table
	Table1 string `json:"table1"`
	// Table2 means that table name of the first table
	Table2 string `json:"table2"`
}

// SwapTable swaps 2 tables.
func (c *Client) SwapTable(ctx context.Context, dbName string, tblName1 string, tblName2 string) (*SwapInfo, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&SwapInfo{}).Post(c.baseURL.String() + fmt.Sprintf("/v3/table/swap/%s/%s/%s", dbName, tblName1, tblName2))
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
	return resp.Result().(*SwapInfo), nil
}

// TailTable shows tables last imported records.
// count means how many records you want to check
func (c *Client) TailTable(ctx context.Context, dbName string, tblName string, count int) (string, error) {
	c.setHeaders(c.httpClient)
	params := map[string]string{"format": "json", "count": strconv.Itoa(count)}
	c.httpClient.SetQueryParams(params)
	resp, err := c.httpClient.R().SetContext(ctx).Get(c.baseURL.String() + fmt.Sprintf("/v3/table/tail/%s/%s", dbName, tblName))
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

// TableDistribution shows the table's curren udp settings
// see https://api-docs.treasuredata.com/en/tools/presto/presto_performance_tuning/#defining-partitioning-for-presto
type TableDistribution struct {
	BucketCount       int      `json:"bucket_count"`
	Columns           []Column `json:"columns"`
	PartitionFunction string   `json:"partition_function"`
}

type Column struct {
	Key  string `json:"key"`
	Name string `json:"name"`
	Type string `json:"type"`
}

// GetDistribution shows table's partition based on UDP
func (c *Client) GetDistribution(ctx context.Context, dbName string, tblName string) (*TableDistribution, error) {
	c.setHeaders(c.httpClient)
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&TableDistribution{}).Get(c.baseURL.String() + fmt.Sprintf("/v3/table/distribution/%s/%s", dbName, tblName))
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

	return resp.Result().(*TableDistribution), nil
}

// ChangeDatabase changes table's database
func (c *Client) ChangeDatabase(ctx context.Context, dbName, tblName, destDBName string) (*TableInfo, error) {
	c.setHeaders(c.httpClient)
	c.httpClient.SetHeader("content-type", "application/x-www-form-urlencoded")
	body := map[string]string{"dest_database_name": destDBName}
	resp, err := c.httpClient.R().SetContext(ctx).SetResult(&TableInfo{}).SetFormData(body).Post(c.baseURL.String() + fmt.Sprintf("/v3/table/change_database/%s/%s", dbName, tblName))
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

	return resp.Result().(*TableInfo), nil
}

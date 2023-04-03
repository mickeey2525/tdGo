//go:generate stringer -type JobType job_type.go

package tdGo

type JobType int

const (
	Presto JobType = iota
	Hive
	BulkLoad
)

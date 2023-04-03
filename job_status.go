//go:generate stringer -type JobStatus job_status.go

package tdGo

type JobStatus int

const (
	Running JobStatus = iota
	Error
	Queued
	Success
	All
)

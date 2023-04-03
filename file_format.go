//go:generate stringer -type FileFormat file_format.go

package tdGo

type FileFormat int

const (
	CSV FileFormat = iota
	TSV
	JSON
	MsgPack
	MsgPackGZ
)

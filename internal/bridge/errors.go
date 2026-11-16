package bridge

import (
	"errors"
	"fmt"
	"strings"
)

// Steps that can fail.
const (
	StageMapping    = "mapping"
	StageMatch      = "match"
	StageDownload   = "download"
	StageUpload     = "upload"
	StageDeadLetter = "dead_letter"
	StageAck        = "ack"
)

// StepError is a failure in one step of the pipeline.
type StepError struct {
	Step       string
	HTTPStatus int
	FTPCode    int
	Err        error
}

func (e *StepError) Error() string { return e.Err.Error() }
func (e *StepError) Unwrap() error { return e.Err }

// Code returns the HTTP status or FTP reply code, or 0.
func (e *StepError) Code() int {
	if e.HTTPStatus != 0 {
		return e.HTTPStatus
	}
	return e.FTPCode
}

func stepErr(step string, err error) *StepError {
	return &StepError{Step: step, Err: err}
}

// ErrorChain returns a readable description of err: the failed step, the
// message and every wrapped error with its Go type, one per line.
func ErrorChain(err error) string {
	var b strings.Builder
	var se *StepError
	if errors.As(err, &se) {
		fmt.Fprintf(&b, "step: %s\n", se.Step)
		if se.HTTPStatus != 0 {
			fmt.Fprintf(&b, "http_status: %d\n", se.HTTPStatus)
		}
		if se.FTPCode != 0 {
			fmt.Fprintf(&b, "ftp_code: %d\n", se.FTPCode)
		}
	}
	fmt.Fprintf(&b, "error: %s", err.Error())
	writeChain(&b, err, 0)
	return b.String()
}

func writeChain(b *strings.Builder, err error, depth int) {
	if err == nil || depth > 32 {
		return
	}
	if _, ok := err.(*StepError); !ok {
		fmt.Fprintf(b, "\n%s%T: %s", strings.Repeat("  ", depth), err, err.Error())
		depth++
	}
	switch u := err.(type) {
	case interface{ Unwrap() []error }:
		for _, e := range u.Unwrap() {
			writeChain(b, e, depth)
		}
	case interface{ Unwrap() error }:
		writeChain(b, u.Unwrap(), depth)
	}
}

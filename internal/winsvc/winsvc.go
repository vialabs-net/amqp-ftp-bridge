// Package winsvc runs the bridge as a Windows service and writes to the
// Windows Event Log. On other systems the functions report that they are not
// available and the event log does nothing.
package winsvc

// Name is the service name and the Event Log source.
const Name = "amqp-ftp-bridge"

// DisplayName is shown in the services console.
const DisplayName = "AMQP FTP Bridge"

// EventLog receives start, stop and fatal error messages.
type EventLog interface {
	Info(msg string)
	Error(msg string)
	Close() error
}

type nopLog struct{}

func (nopLog) Info(string)  {}
func (nopLog) Error(string) {}
func (nopLog) Close() error { return nil }

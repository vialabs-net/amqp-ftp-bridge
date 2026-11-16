package bridge

import "time"

// Observer receives the events that metrics are built from. The Prometheus
// implementation lives in package metrics. Another backend (for example an
// OTLP exporter) implements this interface; Multi combines several.
type Observer interface {
	Received(queue string, redelivered bool)
	InFlight(queue string, delta int)
	// Stage reports a finished stage: download, upload or total. bytes is 0
	// for total.
	Stage(queue, stage string, d time.Duration, bytes int64)
	// Error reports a failed stage with its HTTP or FTP code, or 0.
	Error(queue, stage string, code int)
	// Done reports the final result of a message: uploaded, dead_lettered or
	// requeued.
	Done(queue, result string)
	// FTPResult reports whether the last FTP upload attempt succeeded.
	FTPResult(ok bool)
	AMQPConnected(connected bool)
	AMQPReconnect()
}

// NopObserver ignores every event.
type NopObserver struct{}

func (NopObserver) Received(string, bool)                      {}
func (NopObserver) InFlight(string, int)                       {}
func (NopObserver) Stage(string, string, time.Duration, int64) {}
func (NopObserver) Error(string, string, int)                  {}
func (NopObserver) Done(string, string)                        {}
func (NopObserver) FTPResult(bool)                             {}
func (NopObserver) AMQPConnected(bool)                         {}
func (NopObserver) AMQPReconnect()                             {}

// Multi sends every event to each observer.
type Multi []Observer

func (m Multi) Received(q string, r bool) {
	for _, o := range m {
		o.Received(q, r)
	}
}

func (m Multi) InFlight(q string, d int) {
	for _, o := range m {
		o.InFlight(q, d)
	}
}

func (m Multi) Stage(q, s string, d time.Duration, b int64) {
	for _, o := range m {
		o.Stage(q, s, d, b)
	}
}

func (m Multi) Error(q, s string, c int) {
	for _, o := range m {
		o.Error(q, s, c)
	}
}

func (m Multi) Done(q, r string) {
	for _, o := range m {
		o.Done(q, r)
	}
}

func (m Multi) FTPResult(ok bool) {
	for _, o := range m {
		o.FTPResult(ok)
	}
}

func (m Multi) AMQPConnected(c bool) {
	for _, o := range m {
		o.AMQPConnected(c)
	}
}

func (m Multi) AMQPReconnect() {
	for _, o := range m {
		o.AMQPReconnect()
	}
}

// Package metrics exposes Prometheus metrics, /healthz and /readyz.
package metrics

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Prometheus implements bridge.Observer with Prometheus metrics. It also
// tracks the readiness state.
type Prometheus struct {
	Registry *prometheus.Registry

	messages    *prometheus.CounterVec
	redelivered *prometheus.CounterVec
	duration    *prometheus.HistogramVec
	bytes       *prometheus.CounterVec
	errors      *prometheus.CounterVec
	inflight    *prometheus.GaugeVec
	lastSuccess *prometheus.GaugeVec
	connected   prometheus.Gauge
	reconnects  prometheus.Counter
	amqpUp      atomic.Bool
	ftpFailed   atomic.Bool
}

// New registers every metric, with the Go and process collectors, on a new
// registry.
func New(version, commit string) *Prometheus {
	r := prometheus.NewRegistry()
	p := &Prometheus{
		Registry: r,
		messages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "bridge_messages_total", Help: "Messages handled, by final result.",
		}, []string{"queue", "result"}),
		redelivered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "bridge_redelivered_total", Help: "Messages received with the redelivered flag.",
		}, []string{"queue"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "bridge_stage_duration_seconds", Help: "Duration of the download, upload and total stages.",
			Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300},
		}, []string{"queue", "stage"}),
		bytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "bridge_bytes_total", Help: "Bytes downloaded and uploaded.",
		}, []string{"queue", "stage"}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "bridge_errors_total", Help: "Failures by stage and HTTP or FTP code (0 when none).",
		}, []string{"queue", "stage", "code"}),
		inflight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "bridge_inflight", Help: "Messages being processed.",
		}, []string{"queue"}),
		lastSuccess: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "bridge_last_success_timestamp_seconds", Help: "Unix time of the last uploaded message.",
		}, []string{"queue"}),
		connected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "bridge_amqp_connected", Help: "1 when connected to RabbitMQ.",
		}),
		reconnects: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "bridge_amqp_reconnects_total", Help: "Reconnection attempts to RabbitMQ.",
		}),
	}
	buildInfo := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "bridge_build_info", Help: "Build information.",
	}, []string{"version", "commit"})
	buildInfo.WithLabelValues(version, commit).Set(1)
	r.MustRegister(p.messages, p.redelivered, p.duration, p.bytes, p.errors, p.inflight,
		p.lastSuccess, p.connected, p.reconnects, buildInfo,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return p
}

// Received implements bridge.Observer.
func (p *Prometheus) Received(queue string, redelivered bool) {
	if redelivered {
		p.redelivered.WithLabelValues(queue).Inc()
	}
}

// InFlight implements bridge.Observer.
func (p *Prometheus) InFlight(queue string, delta int) {
	p.inflight.WithLabelValues(queue).Add(float64(delta))
}

// Stage implements bridge.Observer.
func (p *Prometheus) Stage(queue, stage string, d time.Duration, bytes int64) {
	p.duration.WithLabelValues(queue, stage).Observe(d.Seconds())
	if stage != "total" {
		p.bytes.WithLabelValues(queue, stage).Add(float64(bytes))
	}
}

// Error implements bridge.Observer.
func (p *Prometheus) Error(queue, stage string, code int) {
	p.errors.WithLabelValues(queue, stage, strconv.Itoa(code)).Inc()
}

// Done implements bridge.Observer.
func (p *Prometheus) Done(queue, result string) {
	p.messages.WithLabelValues(queue, result).Inc()
	if result == "uploaded" {
		p.lastSuccess.WithLabelValues(queue).SetToCurrentTime()
	}
}

// FTPResult implements bridge.Observer.
func (p *Prometheus) FTPResult(ok bool) { p.ftpFailed.Store(!ok) }

// AMQPConnected implements bridge.Observer.
func (p *Prometheus) AMQPConnected(c bool) {
	p.amqpUp.Store(c)
	if c {
		p.connected.Set(1)
	} else {
		p.connected.Set(0)
	}
}

// AMQPReconnect implements bridge.Observer.
func (p *Prometheus) AMQPReconnect() { p.reconnects.Inc() }

// Ready reports whether RabbitMQ is connected and the last FTP attempt, if
// any, succeeded, with the reason when it is not.
func (p *Prometheus) Ready() (bool, string) {
	switch {
	case !p.amqpUp.Load():
		return false, "amqp not connected"
	case p.ftpFailed.Load():
		return false, "last ftp upload failed"
	}
	return true, "ok"
}

// Handler serves /metrics, /healthz and /readyz.
func (p *Prometheus) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(p.Registry, promhttp.HandlerOpts{Registry: p.Registry}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		ok, reason := p.Ready()
		if !ok {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_, _ = w.Write([]byte(reason + "\n"))
	})
	return mux
}

// Serve listens on addr until ctx is cancelled. It returns once the listener
// is open; errors after that are sent to errc.
func (p *Prometheus) Serve(ctx context.Context, addr string) (net.Addr, <-chan error, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	srv := &http.Server{Handler: p.Handler(), ReadHeaderTimeout: 10 * time.Second}
	errc := make(chan error, 1)
	go func() {
		err := srv.Serve(ln)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errc <- err
	}()
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	return ln.Addr(), errc, nil
}

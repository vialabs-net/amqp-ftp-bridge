package bridge

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Bridge keeps a connection to RabbitMQ and runs one consumer per route.
type Bridge struct {
	// URL is the AMQP URL with credentials.
	URL               string
	Heartbeat         time.Duration
	ReconnectMin      time.Duration
	ReconnectMax      time.Duration
	DeclareDeadLetter bool
	// ShutdownTimeout bounds the wait for in-flight messages on shutdown.
	ShutdownTimeout time.Duration
	Processors      []*Processor
	Log             *slog.Logger
	Obs             Observer
}

func (b *Bridge) obs() Observer {
	if b.Obs == nil {
		return NopObserver{}
	}
	return b.Obs
}

// Run consumes until ctx is cancelled, reconnecting with exponential backoff.
// On cancellation it stops consuming, waits for in-flight messages and
// returns nil.
func (b *Bridge) Run(ctx context.Context) error {
	minWait, maxWait := b.ReconnectMin, b.ReconnectMax
	if minWait <= 0 {
		minWait = time.Second
	}
	if maxWait <= 0 {
		maxWait = 60 * time.Second
	}
	wait := minWait
	first := true
	for {
		connected, err := b.session(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if connected {
			wait = minWait
		}
		if !first || connected {
			b.obs().AMQPReconnect()
		}
		first = false
		b.Log.Warn("amqp connection lost, reconnecting", "err", err, "wait", wait.String())
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil
		}
		wait *= 2
		if wait > maxWait {
			wait = maxWait
		}
	}
}

// redact returns the AMQP URL without credentials, for logs.
func redact(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "(invalid url)"
	}
	u.User = nil
	return u.String()
}

// session connects, consumes until the connection or a channel closes or ctx
// is cancelled, and reports whether every consumer was started.
func (b *Bridge) session(ctx context.Context) (bool, error) {
	host, _ := os.Hostname()
	props := amqp.NewConnectionProperties()
	props.SetClientConnectionName("amqp-ftp-bridge@" + host)
	conn, transport, err := dialAMQP(ctx, b.URL, amqp.Config{Heartbeat: b.Heartbeat, Properties: props})
	if err != nil {
		return false, fmt.Errorf("connect to %s: %w", redact(b.URL), err)
	}
	b.obs().AMQPConnected(true)
	b.Log.Info("amqp connected", "url", redact(b.URL))
	defer func() {
		b.obs().AMQPConnected(false)
		_ = transport.Close()
	}()
	connClosed := conn.NotifyClose(make(chan *amqp.Error, 1))

	// Shutdown lets messages finish until the grace period expires. Other
	// session failures cancel work immediately before joining workers.
	work, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	// Setup RPCs must also stop promptly when startup is cancelled.
	stopSetup := context.AfterFunc(ctx, func() { _ = transport.Close() })
	defer stopSetup()

	type consumer struct {
		ch  *amqp.Channel
		tag string
	}
	var consumers []consumer
	var wg sync.WaitGroup
	defer func() {
		if !stopWorkers(cancelWork, transport, &wg, b.shutdownTimeout()) {
			b.Log.Warn("workers did not stop after cancellation")
		}
	}()
	routeDone := make(chan error, len(b.Processors))
	for _, p := range b.Processors {
		ch, deliveries, tag, err := b.setup(conn, p)
		if err != nil {
			// Closing the connection ends the consumers already started.
			_ = transport.Close()
			// Not counted as connected, so that the backoff keeps growing.
			return false, err
		}
		consumers = append(consumers, consumer{ch, tag})
		adapter := newChannel(ch)
		adapter.transport = transport
		p.MaxFrameBytes = conn.Config.FrameSize
		wg.Add(1)
		go func(p *Processor) {
			defer wg.Done()
			for d := range deliveries {
				if ctx.Err() != nil {
					// Shutting down: leave it unacked, the broker requeues it.
					break
				}
				p.Handle(work, adapter, d)
			}
			routeDone <- fmt.Errorf("consumer on %s stopped", p.Route.Queue)
		}(p)
		b.Log.Info("consuming", "queue", p.Route.Queue)
	}

	stopSetup()
	select {
	case <-ctx.Done():
		b.Log.Info("stopping consumers")
		grace, cancel := context.WithTimeout(context.Background(), b.shutdownTimeout())
		defer cancel()
		stop := context.AfterFunc(grace, func() {
			cancelWork()
			_ = transport.Close()
		})
		defer stop()
		for _, c := range consumers {
			_ = c.ch.Cancel(c.tag, false)
		}
		if !waitWorkers(&wg, time.Until(deadlineOf(grace))) {
			b.Log.Warn("shutdown timeout: abandoning in-flight messages")
		}
		return true, nil
	case aerr := <-connClosed:
		if aerr == nil {
			return true, errors.New("connection closed")
		}
		return true, aerr
	case rerr := <-routeDone:
		return true, rerr
	}
}

func (b *Bridge) setup(conn *amqp.Connection, p *Processor) (*amqp.Channel, <-chan amqp.Delivery, string, error) {
	q := p.Route.Queue
	if b.DeclareDeadLetter {
		if err := declareDeadLetter(conn, p.Route); err != nil {
			return nil, nil, "", fmt.Errorf("declare dead-letter for %s: %w", q, err)
		}
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, "", fmt.Errorf("open channel for %s: %w", q, err)
	}
	if err := ch.Qos(1, 0, false); err != nil {
		return nil, nil, "", fmt.Errorf("set prefetch on %s: %w", q, err)
	}
	if err := ch.Confirm(false); err != nil {
		return nil, nil, "", fmt.Errorf("enable confirms on %s: %w", q, err)
	}
	// Generate a unique tag within AMQP's 255-byte limit,
	// independently of queue and host name lengths.
	tag := "afb-" + newUUID()
	deliveries, err := ch.Consume(q, tag, false, false, false, false, nil)
	if err != nil {
		return nil, nil, "", fmt.Errorf("consume %s: %w", q, err)
	}
	return ch, deliveries, tag, nil
}

// declareDeadLetter declares the dead-letter exchange (direct, durable), a
// durable queue named after the routing key, and binds them. It uses its own
// channel, so that a mismatch with existing objects does not close the
// consumer channel.
func declareDeadLetter(conn *amqp.Connection, r Route) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	if err := ch.ExchangeDeclare(r.DeadLetterExchange, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(r.DeadLetterKey, true, false, false, false, nil); err != nil {
		return err
	}
	return ch.QueueBind(r.DeadLetterKey, r.DeadLetterKey, r.DeadLetterExchange, false, nil)
}

// amqpChannel adapts *amqp.Channel, in confirm mode, to Channel.
type amqpChannel struct {
	mu        sync.Mutex
	ch        *amqp.Channel
	returns   chan amqp.Return
	transport net.Conn
}

func newChannel(ch *amqp.Channel) *amqpChannel {
	return &amqpChannel{ch: ch, returns: ch.NotifyReturn(make(chan amqp.Return, 1))}
}

func (a *amqpChannel) Ack(tag uint64) error { return a.ch.Ack(tag, false) }

func (a *amqpChannel) Nack(tag uint64, requeue bool) error { return a.ch.Nack(tag, false, requeue) }

func (a *amqpChannel) PublishConfirmed(ctx context.Context, exchange, key string, p amqp.Publishing) error {
	// The library ignores cancellation while writing publish frames. Closing
	// the shared transport interrupts that write and requeues all unacked work.
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.transport != nil {
		stop := context.AfterFunc(ctx, func() { _ = a.transport.Close() })
		defer stop()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for drained := false; !drained; {
		select {
		case _, open := <-a.returns:
			if !open {
				return amqp.ErrClosed
			}
		default:
			drained = true
		}
	}
	dc, err := a.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key, true, false, p)
	if err != nil {
		return err
	}
	ok, err := dc.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("broker refused the message (nack)")
	}
	select {
	case r, open := <-a.returns:
		if !open {
			return amqp.ErrClosed
		}
		return fmt.Errorf("message returned as unroutable: %d %s", r.ReplyCode, r.ReplyText)
	default:
	}
	return nil
}

func (b *Bridge) shutdownTimeout() time.Duration {
	if b.ShutdownTimeout > 0 {
		return b.ShutdownTimeout
	}
	return 5 * time.Minute
}

func deadlineOf(ctx context.Context) time.Time {
	deadline, _ := ctx.Deadline()
	return deadline
}

// Stop work before joining on every exit path, including partial setup.
func stopWorkers(cancel context.CancelFunc, transport net.Conn, wg *sync.WaitGroup, timeout time.Duration) bool {
	cancel()
	_ = transport.Close()
	return waitWorkers(wg, timeout)
}

func waitWorkers(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	timer := time.NewTimer(max(timeout, 0))
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// dialAMQP bounds DNS/TCP/TLS/AMQP setup and makes all of it cancellable.
func dialAMQP(ctx context.Context, address string, cfg amqp.Config) (*amqp.Connection, net.Conn, error) {
	const timeout = 30 * time.Second
	var transport net.Conn
	stop := func() bool { return true }
	cfg.Dial = func(network, addr string) (net.Conn, error) {
		raw, err := (&net.Dialer{Timeout: timeout}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		transport = &amqpTransport{Conn: raw, timeout: timeout}
		if err := transport.SetDeadline(time.Now().Add(timeout)); err != nil {
			_ = transport.Close()
			return nil, err
		}
		stop = context.AfterFunc(ctx, func() { _ = transport.Close() })
		return transport, nil
	}
	conn, err := amqp.DialConfig(address, cfg)
	stop()
	if err != nil && transport != nil {
		_ = transport.Close()
	}
	return conn, transport, err
}

type amqpTransport struct {
	net.Conn
	timeout time.Duration
}

func (c *amqpTransport) Write(p []byte) (int, error) {
	if err := c.Conn.SetWriteDeadline(time.Now().Add(c.timeout)); err != nil {
		return 0, err
	}
	return c.Conn.Write(p)
}

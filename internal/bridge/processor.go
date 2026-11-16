// Package bridge consumes the configured queues and runs each message through
// download, upload, acknowledgement or dead-lettering.
package bridge

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/vialabs-net/amqp-ftp-bridge/internal/audit"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/config"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/download"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/ftpup"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/mapping"
)

// Message results reported to the Observer.
const (
	ResultUploaded     = "uploaded"
	ResultDeadLettered = "dead_lettered"
	ResultRequeued     = "requeued"
)

// Downloader fetches a file by id into a temp file.
type Downloader interface {
	Fetch(ctx context.Context, id string) (download.Result, error)
	Host() string
}

// Uploader stores a file on the FTP target and returns the reply code.
type Uploader interface {
	Upload(ctx context.Context, folder, name string, r io.Reader) (int, error)
}

// AuditWriter appends audit records.
type AuditWriter interface {
	Write(audit.Record) error
}

// Channel is the part of an AMQP channel the processor uses.
type Channel interface {
	Ack(tag uint64) error
	Nack(tag uint64, requeue bool) error
	// PublishConfirmed publishes and waits for the broker confirmation. It
	// fails when the message is unroutable.
	PublishConfirmed(ctx context.Context, exchange, key string, p amqp.Publishing) error
}

// Route is one queue with its source, target and rules.
type Route struct {
	Queue              string
	SourceName         string
	DeadLetterExchange string
	DeadLetterKey      string
	Mapping            config.Mapping
	Match              *config.Match
	Downloader         Downloader
	Uploader           Uploader
	TempDir            string
}

// Processor handles the messages of one route.
type Processor struct {
	Route            Route
	Audit            AuditWriter
	Log              *slog.Logger
	Obs              Observer
	ErrorHeader      string
	StacktraceHeader string
	// RequeueDelay is waited before a nack with requeue, so that a message
	// that cannot be dead-lettered is not redelivered in a tight loop.
	RequeueDelay time.Duration
	// PublishTimeout bounds the dead-letter publish and its confirmation.
	PublishTimeout time.Duration
	// MaxFrameBytes is the negotiated AMQP frame limit (zero uses 128 KiB).
	MaxFrameBytes int
	Now           func() time.Time
}

type job struct {
	msgID    string
	start    time.Time
	folder   string
	fileName string
	tempPath string
}

func (p *Processor) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func (p *Processor) obs() Observer {
	if p.Obs == nil {
		return NopObserver{}
	}
	return p.Obs
}

func (p *Processor) audit(r audit.Record) {
	r.Queue = p.Route.Queue
	if err := p.Audit.Write(r); err != nil {
		p.Log.Error("audit write failed", "queue", p.Route.Queue, "step", r.Step, "err", err)
	}
}

// Handle processes one delivery and acknowledges, dead-letters or requeues it.
func (p *Processor) Handle(ctx context.Context, ch Channel, d amqp.Delivery) string {
	q := p.Route.Queue
	o := p.obs()
	o.InFlight(q, 1)
	defer o.InFlight(q, -1)

	j := &job{msgID: d.MessageId, start: p.now()}
	if j.msgID == "" {
		j.msgID = newUUID()
	}
	defer func() {
		if j.tempPath != "" {
			_ = os.Remove(j.tempPath)
		}
	}()

	msg, parseErr := mapping.Parse(d.Body, d.Headers)
	matched, event := true, ""
	rec := audit.Record{Step: audit.StepReceived, MsgID: j.msgID, Redelivered: audit.Ptr(d.Redelivered)}
	if m := p.Route.Match; m != nil {
		matched, event = msg.Matches(m.Field, m.Equals)
		rec.Event, rec.Matched = audit.Ptr(event), audit.Ptr(matched)
	}
	if parseErr == nil {
		rec.Body = json.RawMessage(compact(d.Body))
	} else {
		rec.BodyText = audit.Ptr(string(d.Body))
	}
	p.audit(rec)
	o.Received(q, d.Redelivered)

	var err error
	switch {
	case matched:
		err = p.transfer(ctx, j, msg)
	case p.Route.Match.Otherwise == config.OtherwiseUploadField:
		err = p.uploadField(ctx, j, msg)
	default:
		err = stepErr(StageMatch, fmt.Errorf("%s is %q, want %q", p.Route.Match.Field, event, p.Route.Match.Equals))
	}
	if err != nil {
		return p.fail(ctx, ch, d, j, err)
	}

	if err := ch.Ack(d.DeliveryTag); err != nil {
		// The broker redelivers the message: the upload is repeated and overwrites.
		p.Log.Warn("ack failed", "queue", q, "msg_id", j.msgID, "err", err)
		o.Error(q, StageAck, 0)
		o.Done(q, ResultRequeued)
		return ResultRequeued
	}
	total := p.now().Sub(j.start)
	p.audit(audit.Record{Step: audit.StepAcked, MsgID: j.msgID, TotalMs: audit.Ptr(total.Milliseconds())})
	o.Stage(q, "total", total, 0)
	o.Done(q, ResultUploaded)
	return ResultUploaded
}

func (p *Processor) resolveTarget(j *job, msg *mapping.Message) error {
	var err error
	if j.folder, err = msg.String(p.Route.Mapping.Folder); err != nil {
		return stepErr(StageMapping, fmt.Errorf("folder: %w", err))
	}
	if j.fileName, err = msg.String(p.Route.Mapping.FileName); err != nil {
		return stepErr(StageMapping, fmt.Errorf("file_name: %w", err))
	}
	if _, err := ftpup.Join(j.folder, j.fileName); err != nil {
		return stepErr(StageMapping, err)
	}
	return nil
}

func (p *Processor) transfer(ctx context.Context, j *job, msg *mapping.Message) error {
	q := p.Route.Queue
	sourceID, err := msg.String(p.Route.Mapping.SourceID)
	if err != nil {
		return stepErr(StageMapping, fmt.Errorf("source_id: %w", err))
	}
	if err := p.resolveTarget(j, msg); err != nil {
		return err
	}

	start := p.now()
	res, err := p.Route.Downloader.Fetch(ctx, sourceID)
	if err != nil {
		se := stepErr(StageDownload, err)
		se.HTTPStatus = res.Status
		return se
	}
	j.tempPath = res.Path
	dur := p.now().Sub(start)
	p.audit(audit.Record{
		Step: audit.StepDownloaded, MsgID: j.msgID, Source: p.Route.SourceName, SourceID: sourceID,
		Host: res.Host, HTTPStatus: res.Status, Bytes: audit.Ptr(res.Bytes), SHA256: res.SHA256,
		Ms: audit.Ptr(dur.Milliseconds()),
	})
	p.obs().Stage(q, StageDownload, dur, res.Bytes)
	return p.upload(ctx, j, res.Path)
}

func (p *Processor) uploadField(ctx context.Context, j *job, msg *mapping.Message) error {
	data, err := msg.JSON(p.Route.Match.UploadField)
	if err != nil {
		return stepErr(StageMapping, fmt.Errorf("upload_field: %w", err))
	}
	if err := p.resolveTarget(j, msg); err != nil {
		return err
	}
	if err := os.MkdirAll(p.Route.TempDir, 0o750); err != nil {
		return stepErr(StageUpload, err)
	}
	f, err := os.CreateTemp(p.Route.TempDir, "field-*.tmp")
	if err != nil {
		return stepErr(StageUpload, err)
	}
	j.tempPath = f.Name()
	_, err = f.Write(data)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return stepErr(StageUpload, err)
	}
	return p.upload(ctx, j, j.tempPath)
}

func (p *Processor) upload(ctx context.Context, j *job, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return stepErr(StageUpload, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return stepErr(StageUpload, err)
	}
	start := p.now()
	code, err := p.Route.Uploader.Upload(ctx, j.folder, j.fileName, f)
	p.obs().FTPResult(err == nil)
	if err != nil {
		se := stepErr(StageUpload, err)
		se.FTPCode = code
		return se
	}
	dur := p.now().Sub(start)
	p.audit(audit.Record{
		Step: audit.StepUploaded, MsgID: j.msgID, Folder: j.folder, FileName: j.fileName,
		FTPCode: code, Bytes: audit.Ptr(info.Size()), Ms: audit.Ptr(dur.Milliseconds()),
	})
	p.obs().Stage(p.Route.Queue, StageUpload, dur, info.Size())
	return nil
}

// fail publishes the original message to the dead-letter exchange with the
// error headers, then acks it. When the publish fails the message is requeued.
func (p *Processor) fail(ctx context.Context, ch Channel, d amqp.Delivery, j *job, err error) string {
	q := p.Route.Queue
	var se *StepError
	if !errors.As(err, &se) {
		se = stepErr("unknown", err)
	}
	p.obs().Error(q, se.Step, se.Code())

	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	delete(headers, p.ErrorHeader)
	delete(headers, p.StacktraceHeader)
	pub := amqp.Publishing{
		Headers:         headers,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode:    amqp.Persistent,
		Priority:        d.Priority,
		CorrelationId:   d.CorrelationId,
		ReplyTo:         d.ReplyTo,
		MessageId:       d.MessageId,
		Timestamp:       d.Timestamp,
		Type:            d.Type,
		AppId:           d.AppId,
		Body:            d.Body,
	}
	if d.Expiration != "" {
		previous, exists := headers["x-original-expiration"]
		headers["x-original-expiration"] = d.Expiration
		if p.frameBytes(&pub) > p.frameLimit() {
			if exists {
				headers["x-original-expiration"] = previous
			} else {
				delete(headers, "x-original-expiration")
			}
		}
	}
	p.addDiagnostics(&pub, err)
	timeout := p.PublishTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	pctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if perr := ch.PublishConfirmed(pctx, p.Route.DeadLetterExchange, p.Route.DeadLetterKey, pub); perr != nil {
		p.Log.Error("dead-letter publish failed, requeueing", "queue", q, "msg_id", j.msgID,
			"exchange", p.Route.DeadLetterExchange, "err", perr, "cause", err)
		p.obs().Error(q, StageDeadLetter, 0)
		if p.RequeueDelay > 0 {
			select {
			case <-time.After(p.RequeueDelay):
			case <-ctx.Done():
			}
		}
		if nerr := ch.Nack(d.DeliveryTag, true); nerr != nil {
			p.Log.Warn("nack failed", "queue", q, "msg_id", j.msgID, "err", nerr)
		}
		p.obs().Done(q, ResultRequeued)
		return ResultRequeued
	}
	rec := audit.Record{
		Step: audit.StepDeadLettered, MsgID: j.msgID, FailedStep: se.Step, Error: err.Error(),
		Folder: j.folder, FileName: j.fileName,
	}
	if se.HTTPStatus != 0 {
		rec.HTTPStatus = se.HTTPStatus
	}
	if se.FTPCode != 0 {
		rec.FTPCode = se.FTPCode
	}
	p.audit(rec)
	if aerr := ch.Ack(d.DeliveryTag); aerr != nil {
		p.Log.Warn("ack after dead-letter failed", "queue", q, "msg_id", j.msgID, "err", aerr)
	}
	p.Log.Info("message dead-lettered", "queue", q, "msg_id", j.msgID, "step", se.Step, "err", err)
	p.obs().Done(q, ResultDeadLettered)
	return ResultDeadLettered
}

// Header frames cannot be split. Keep the original headers and use only the
// remaining frame space for diagnostics, with a 2 KiB cap per diagnostic.
func (p *Processor) addDiagnostics(pub *amqp.Publishing, err error) {
	available := p.frameLimit() - p.frameBytes(pub) - len(p.ErrorHeader) - len(p.StacktraceHeader) - 12
	if available < 0 {
		return // Original headers leave no room for diagnostic fields.
	}
	budget := min(available/2, 2048)
	for key, value := range map[string]string{p.ErrorHeader: err.Error(), p.StacktraceHeader: ErrorChain(err)} {
		if len(value) > budget {
			value = strings.ToValidUTF8(value[:budget], "")
		}
		pub.Headers[key] = value
	}
}

func (p *Processor) frameLimit() int {
	if p.MaxFrameBytes > 0 {
		return p.MaxFrameBytes
	}
	return 128 * 1024
}

func (p *Processor) frameBytes(pub *amqp.Publishing) int {
	// Conservative bound for the frame, content header and scalar properties.
	size := 64 + fieldSize(pub.Headers)
	for _, s := range []string{pub.ContentType, pub.ContentEncoding, pub.CorrelationId, pub.ReplyTo, pub.Expiration, pub.MessageId, pub.Type, pub.UserId, pub.AppId} {
		size += len(s) + 1
	}
	return size
}

// fieldSize conservatively bounds the AMQP field encoding, including its
// type byte. Nine bytes cover every fixed-size AMQP scalar.
func fieldSize(value any) int {
	switch v := value.(type) {
	case string:
		return 5 + len(v)
	case []byte:
		return 5 + len(v)
	case amqp.Table:
		n := 5
		for key, value := range v {
			n += 1 + len(key) + fieldSize(value)
		}
		return n
	case []any:
		n := 5
		for _, value := range v {
			n += fieldSize(value)
		}
		return n
	default:
		return 9
	}
}

func compact(b []byte) []byte {
	var buf bytes.Buffer
	if err := json.Compact(&buf, b); err != nil {
		return b
	}
	return buf.Bytes()
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

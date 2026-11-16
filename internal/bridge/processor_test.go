package bridge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/vialabs-net/amqp-ftp-bridge/internal/audit"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/config"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/download"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/ftpup"
)

type memAudit struct {
	mu   sync.Mutex
	recs []audit.Record
	err  error
}

func (m *memAudit) Write(r audit.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recs = append(m.recs, r)
	return m.err
}

func (m *memAudit) steps() string {
	var s []string
	for _, r := range m.recs {
		s = append(s, r.Step)
	}
	return strings.Join(s, ",")
}

type fakeChannel struct {
	acked, nacked []uint64
	requeue       bool
	published     []amqp.Publishing
	exchange, key string
	publishErr    error
	ackErr        error
}

func (f *fakeChannel) Ack(tag uint64) error { f.acked = append(f.acked, tag); return f.ackErr }
func (f *fakeChannel) Nack(tag uint64, requeue bool) error {
	f.nacked, f.requeue = append(f.nacked, tag), requeue
	return nil
}
func (f *fakeChannel) PublishConfirmed(_ context.Context, ex, key string, p amqp.Publishing) error {
	if f.publishErr != nil {
		return f.publishErr
	}
	f.exchange, f.key = ex, key
	f.published = append(f.published, p)
	return nil
}

type fakeDownloader struct {
	dir     string
	content string
	status  int
	err     error
	gotID   string
}

func (f *fakeDownloader) Host() string { return "files.example.com" }
func (f *fakeDownloader) Fetch(_ context.Context, id string) (download.Result, error) {
	f.gotID = id
	if f.err != nil {
		return download.Result{Host: f.Host(), Status: f.status}, f.err
	}
	p := filepath.Join(f.dir, "dl.tmp")
	if err := os.WriteFile(p, []byte(f.content), 0o600); err != nil {
		return download.Result{}, err
	}
	return download.Result{Path: p, Host: f.Host(), Status: 200, Bytes: int64(len(f.content)), SHA256: "abc"}, nil
}

type fakeUploader struct {
	folder, name, content string
	code                  int
	err                   error
}

func (f *fakeUploader) Upload(_ context.Context, folder, name string, r io.Reader) (int, error) {
	b, _ := io.ReadAll(r)
	f.folder, f.name, f.content = folder, name, string(b)
	if f.err != nil {
		return f.code, f.err
	}
	return 226, nil
}

type countObs struct {
	NopObserver
	done []string
	errs []string
	ftp  []bool
}

func (c *countObs) Done(_, r string) { c.done = append(c.done, r) }
func (c *countObs) Error(_, s string, code int) {
	c.errs = append(c.errs, fmt.Sprintf("%s/%d", s, code))
}
func (c *countObs) FTPResult(ok bool) { c.ftp = append(c.ftp, ok) }

const body = `{"event":"upload","data":{"_file":{"id":"F-1","name":"report.pdf"},"meta":{"k":"v"}}}`

type fixture struct {
	p    *Processor
	ch   *fakeChannel
	dl   *fakeDownloader
	up   *fakeUploader
	aud  *memAudit
	obs  *countObs
	dir  string
	sent string
}

func newFixture(t *testing.T, match *config.Match) *fixture {
	t.Helper()
	dir := t.TempDir()
	f := &fixture{ch: &fakeChannel{}, dl: &fakeDownloader{dir: dir, content: "PDF"}, up: &fakeUploader{}, aud: &memAudit{}, obs: &countObs{}, dir: dir}
	f.p = &Processor{
		Route: Route{
			Queue: "files.upload", SourceName: "files-api",
			DeadLetterExchange: "files.upload_deadLetter", DeadLetterKey: "files.upload_deadLetter",
			Mapping:    config.Mapping{SourceID: "data._file.id", Folder: "header:DestinationPath", FileName: "data._file.name"},
			Match:      match,
			Downloader: f.dl, Uploader: f.up, TempDir: filepath.Join(dir, "tmp"),
		},
		Audit: f.aud, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Obs: f.obs,
		ErrorHeader: "x-error", StacktraceHeader: "x-error-stacktrace",
	}
	return f
}

func (f *fixture) handle(d amqp.Delivery) string {
	f.sent = string(d.Body)
	return f.p.Handle(context.Background(), f.ch, d)
}

func delivery(b string) amqp.Delivery {
	return amqp.Delivery{
		DeliveryTag: 7, MessageId: "msg-1", Body: []byte(b), ContentType: "application/json",
		Headers: amqp.Table{"DestinationPath": "inbox/2026", "trace": "t-1"},
	}
}

var matchUpload = &config.Match{Field: "event", Equals: "upload", Otherwise: config.OtherwiseDeadLetter}

func TestHappyPath(t *testing.T) {
	f := newFixture(t, matchUpload)
	if got := f.handle(delivery(body)); got != ResultUploaded {
		t.Fatalf("result = %s", got)
	}
	if f.aud.steps() != "received,downloaded,uploaded,acked" {
		t.Errorf("steps = %s", f.aud.steps())
	}
	if len(f.ch.acked) != 1 || f.ch.acked[0] != 7 || len(f.ch.published) != 0 {
		t.Errorf("acked %v published %d", f.ch.acked, len(f.ch.published))
	}
	if f.dl.gotID != "F-1" || f.up.folder != "inbox/2026" || f.up.name != "report.pdf" || f.up.content != "PDF" {
		t.Errorf("dl %q up %+v", f.dl.gotID, f.up)
	}
	if _, err := os.Stat(filepath.Join(f.dir, "dl.tmp")); !os.IsNotExist(err) {
		t.Error("temp file not removed")
	}
	r := f.aud.recs
	if *r[0].Event != "upload" || !*r[0].Matched || *r[0].Redelivered || string(r[0].Body) != body || r[0].MsgID != "msg-1" || r[0].Queue != "files.upload" {
		t.Errorf("received = %+v", r[0])
	}
	if r[1].Source != "files-api" || r[1].SourceID != "F-1" || r[1].Host != "files.example.com" || r[1].HTTPStatus != 200 || *r[1].Bytes != 3 || r[1].Ms == nil {
		t.Errorf("downloaded = %+v", r[1])
	}
	if r[2].Folder != "inbox/2026" || r[2].FileName != "report.pdf" || r[2].FTPCode != 226 || *r[2].Bytes != 3 {
		t.Errorf("uploaded = %+v", r[2])
	}
	if r[3].TotalMs == nil {
		t.Errorf("acked = %+v", r[3])
	}
	if strings.Join(f.obs.done, ",") != ResultUploaded || len(f.obs.ftp) != 1 || !f.obs.ftp[0] {
		t.Errorf("obs = %+v", f.obs)
	}
}

func checkDeadLetter(t *testing.T, f *fixture, step, errPart string) audit.Record {
	t.Helper()
	if len(f.ch.published) != 1 {
		t.Fatalf("published %d", len(f.ch.published))
	}
	p := f.ch.published[0]
	if f.ch.exchange != "files.upload_deadLetter" || f.ch.key != "files.upload_deadLetter" {
		t.Errorf("exchange %q key %q", f.ch.exchange, f.ch.key)
	}
	if string(p.Body) != f.sent {
		t.Errorf("body changed: %s", p.Body)
	}
	if p.Headers["trace"] != "t-1" || p.Headers["DestinationPath"] != "inbox/2026" {
		t.Errorf("original headers lost: %v", p.Headers)
	}
	if e, _ := p.Headers["x-error"].(string); !strings.Contains(e, errPart) {
		t.Errorf("x-error = %q, want %q", e, errPart)
	}
	if st, _ := p.Headers["x-error-stacktrace"].(string); !strings.Contains(st, "step: "+step) {
		t.Errorf("stacktrace = %q", st)
	}
	if p.MessageId != "msg-1" || p.ContentType != "application/json" || p.DeliveryMode != amqp.Persistent {
		t.Errorf("properties = %+v", p)
	}
	if len(f.ch.acked) != 1 || len(f.ch.nacked) != 0 {
		t.Errorf("acked %v nacked %v", f.ch.acked, f.ch.nacked)
	}
	last := f.aud.recs[len(f.aud.recs)-1]
	if last.Step != audit.StepDeadLettered || last.FailedStep != step || !strings.Contains(last.Error, errPart) {
		t.Errorf("dead_lettered = %+v", last)
	}
	return last
}

func TestDownload404(t *testing.T) {
	f := newFixture(t, nil)
	f.dl.err, f.dl.status = &download.StatusError{Status: 404, Host: "files.example.com"}, 404
	if got := f.handle(delivery(body)); got != ResultDeadLettered {
		t.Fatalf("result = %s", got)
	}
	rec := checkDeadLetter(t, f, StageDownload, "HTTP 404")
	if rec.HTTPStatus != 404 || rec.FileName != "report.pdf" {
		t.Errorf("record = %+v", rec)
	}
	if f.aud.steps() != "received,dead_lettered" {
		t.Errorf("steps = %s", f.aud.steps())
	}
	if f.aud.recs[0].Event != nil {
		t.Error("event must be absent without match rule")
	}
	if strings.Join(f.obs.errs, ",") != "download/404" {
		t.Errorf("errs = %v", f.obs.errs)
	}
	st := f.ch.published[0].Headers["x-error-stacktrace"].(string)
	if !strings.Contains(st, "http_status: 404") || !strings.Contains(st, "*download.StatusError") {
		t.Errorf("stacktrace = %s", st)
	}
}

func TestUploadFailure(t *testing.T) {
	f := newFixture(t, matchUpload)
	f.up.err = &ftpup.Error{Op: "stor", Code: 550, Err: &textproto.Error{Code: 550, Msg: "denied"}}
	f.up.code = 550
	f.handle(delivery(body))
	rec := checkDeadLetter(t, f, StageUpload, "550")
	if rec.FTPCode != 550 {
		t.Errorf("ftp code = %d", rec.FTPCode)
	}
	if f.aud.steps() != "received,downloaded,dead_lettered" {
		t.Errorf("steps = %s", f.aud.steps())
	}
	if len(f.obs.ftp) != 1 || f.obs.ftp[0] {
		t.Error("ftp result must be false")
	}
}

func TestUnmatchedDeadLetter(t *testing.T) {
	f := newFixture(t, matchUpload)
	f.handle(delivery(strings.Replace(body, `"upload"`, `"delete"`, 1)))
	checkDeadLetter(t, f, StageMatch, `event is "delete", want "upload"`)
	if r := f.aud.recs[0]; *r.Event != "delete" || *r.Matched {
		t.Errorf("received = %+v", r)
	}
	if f.dl.gotID != "" {
		t.Error("must not download")
	}
}

func TestUnmatchedUploadField(t *testing.T) {
	f := newFixture(t, &config.Match{Field: "event", Equals: "upload", Otherwise: config.OtherwiseUploadField, UploadField: "data.meta"})
	got := f.handle(delivery(strings.Replace(body, `"upload"`, `"update"`, 1)))
	if got != ResultUploaded || f.up.content != `{"k":"v"}` || f.up.name != "report.pdf" {
		t.Errorf("result %s upload %+v", got, f.up)
	}
	if f.aud.steps() != "received,uploaded,acked" {
		t.Errorf("steps = %s", f.aud.steps())
	}
	if entries, _ := os.ReadDir(f.p.Route.TempDir); len(entries) != 0 {
		t.Error("temp file not removed")
	}

	f = newFixture(t, &config.Match{Field: "event", Equals: "upload", Otherwise: config.OtherwiseUploadField, UploadField: "data.missing"})
	f.handle(delivery(strings.Replace(body, `"upload"`, `"update"`, 1)))
	checkDeadLetter(t, f, StageMapping, "upload_field")
}

func TestMappingFailures(t *testing.T) {
	for name, tc := range map[string]struct {
		mut  func(*fixture)
		want string
	}{
		"source_id": {func(f *fixture) { f.p.Route.Mapping.SourceID = "data.nope" }, "source_id"},
		"folder":    {func(f *fixture) { f.p.Route.Mapping.Folder = "header:Nope" }, "folder"},
		"file_name": {func(f *fixture) { f.p.Route.Mapping.FileName = "data.nope" }, "file_name"},
		"path":      {func(f *fixture) { f.p.Route.Mapping.Folder = "literal:../etc" }, "invalid folder"},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, nil)
			tc.mut(f)
			f.handle(delivery(body))
			checkDeadLetter(t, f, StageMapping, tc.want)
		})
	}
}

func TestNotJSONBody(t *testing.T) {
	f := newFixture(t, nil)
	d := delivery("not json")
	d.MessageId = ""
	f.handle(d)
	r := f.aud.recs[0]
	if r.BodyText == nil || *r.BodyText != "not json" || r.Body != nil {
		t.Errorf("received = %+v", r)
	}
	if len(r.MsgID) != 36 || f.aud.recs[1].MsgID != r.MsgID {
		t.Errorf("generated msg id %q", r.MsgID)
	}
	if string(f.ch.published[0].Body) != "not json" {
		t.Error("original body must be dead-lettered")
	}
}

func TestDeadLetterPublishFails(t *testing.T) {
	f := newFixture(t, nil)
	f.dl.err = errors.New("boom")
	f.ch.publishErr = errors.New("channel closed")
	f.p.RequeueDelay = time.Millisecond
	if got := f.handle(delivery(body)); got != ResultRequeued {
		t.Fatalf("result = %s", got)
	}
	if len(f.ch.acked) != 0 || len(f.ch.nacked) != 1 || !f.ch.requeue {
		t.Errorf("acked %v nacked %v requeue %v", f.ch.acked, f.ch.nacked, f.ch.requeue)
	}
	if f.aud.steps() != "received" {
		t.Errorf("steps = %s", f.aud.steps())
	}
}

func TestAckFails(t *testing.T) {
	f := newFixture(t, nil)
	f.ch.ackErr = errors.New("closed")
	f.aud.err = errors.New("disk full") // logged, does not stop the message
	if got := f.handle(delivery(body)); got != ResultRequeued {
		t.Errorf("result = %s", got)
	}
}

func TestErrorChain(t *testing.T) {
	inner := &textproto.Error{Code: 530, Msg: "Login incorrect."}
	err := &StepError{Step: StageUpload, FTPCode: 530, Err: fmt.Errorf("upload: %w", errors.Join(&ftpup.Error{Op: "login", Code: 530, Err: inner}, errors.New("second")))}
	got := ErrorChain(err)
	for _, want := range []string{"step: upload\n", "ftp_code: 530\n", "error: upload: ftp login: 530 Login incorrect.", "*fmt.wrapError", "  *errors.joinError", "    *ftpup.Error", "      *textproto.Error", "    *errors.errorString: second"} {
		if !strings.Contains(got, want) {
			t.Errorf("chain missing %q:\n%s", want, got)
		}
	}
	if err.Code() != 530 || (&StepError{HTTPStatus: 404}).Code() != 404 {
		t.Error("Code")
	}
	if ErrorChain(errors.New("plain")) != "error: plain\n*errors.errorString: plain" {
		t.Errorf("%q", ErrorChain(errors.New("plain")))
	}
}

func TestMultiObserver(t *testing.T) {
	a, b := &countObs{}, &countObs{}
	m := Multi{a, b}
	m.Received("q", true)
	m.InFlight("q", 1)
	m.Stage("q", "download", time.Second, 1)
	m.Error("q", "download", 1)
	m.Done("q", "uploaded")
	m.FTPResult(true)
	m.AMQPConnected(true)
	m.AMQPReconnect()
	if len(a.done) != 1 || len(b.errs) != 1 || len(b.ftp) != 1 {
		t.Error("multi")
	}
	var p Processor
	if _, ok := p.obs().(NopObserver); !ok {
		t.Error("default observer")
	}
	if p.now().IsZero() {
		t.Error("now")
	}
	if (&Bridge{}).obs() == nil {
		t.Error("bridge observer")
	}
}

func TestRedact(t *testing.T) {
	if got := redact("amqps://user:pass@rabbitmq.example.internal:5671/vh"); got != "amqps://rabbitmq.example.internal:5671/vh" {
		t.Error(got)
	}
	if redact("::") != "(invalid url)" {
		t.Error("invalid")
	}
}

package cli

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/vialabs-net/amqp-ftp-bridge/internal/config"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/creds"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/download"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/ftpup"
)

// cmdCheck connects to RabbitMQ (and checks every queue without consuming),
// logs in to every FTP target and requests every source prefix.
func cmdCheck(args []string, stdout, stderr io.Writer) error {
	fs, cfgPath := newFlags("check", stderr)
	timeout := fs.Duration("timeout", 30*time.Second, "timeout for each check")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	secrets, err := creds.LoadAll(newStore(cfg.DataDir), cfg.Credentials())
	if err != nil {
		return err
	}

	failed := 0
	report := func(what, target string, err error, detail string) {
		status := "OK  "
		if err != nil {
			status, detail = "FAIL", err.Error()
			failed++
		}
		fmt.Fprintf(stdout, "%s %-7s %-40s %s\n", status, what, target, detail)
	}

	checkAMQP(cfg, secrets, *timeout, report)
	for _, f := range cfg.FTP {
		c := secrets[f.Credential]
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		err := (&ftpup.Client{Address: f.Address, User: c.User, Password: c.Password, Timeout: *timeout}).Check(ctx)
		cancel()
		report("ftp", f.Name+" "+f.Address, err, "login ok")
	}
	for _, s := range cfg.Sources {
		c := secrets[s.Credential]
		d := download.New(s.URLPrefix, c.User, c.Password, cfg.TempDir, *timeout)
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		status, err := d.Check(ctx)
		cancel()
		if err == nil && (status == http.StatusUnauthorized || status == http.StatusForbidden || status >= 500) {
			err = fmt.Errorf("HTTP %d %s", status, http.StatusText(status))
		}
		report("source", s.Name+" "+d.Host(), err, fmt.Sprintf("HTTP %d", status))
	}
	if failed > 0 {
		fmt.Fprintf(stderr, "%d check(s) failed\n", failed)
		return errReported
	}
	return nil
}

func checkAMQP(cfg *config.Config, secrets map[string]creds.Credential, timeout time.Duration, report func(string, string, error, string)) {
	u, _ := url.Parse(cfg.AMQP.URL)
	host := u.Host
	c := secrets[cfg.AMQP.Credential]
	u.User = url.UserPassword(c.User, c.Password)
	conn, err := amqp.DialConfig(u.String(), amqp.Config{Dial: amqp.DefaultDial(timeout)})
	report("amqp", host, err, "connected")
	if err != nil {
		return
	}
	defer conn.Close()
	for _, r := range cfg.Routes {
		ch, err := conn.Channel()
		if err != nil {
			report("queue", r.Queue, err, "")
			continue
		}
		// Passive: fails when the queue does not exist, never creates it.
		q, err := ch.QueueDeclarePassive(r.Queue, true, false, false, false, nil)
		report("queue", r.Queue, err, fmt.Sprintf("%d message(s), %d consumer(s)", q.Messages, q.Consumers))
		_ = ch.Close()
	}
}

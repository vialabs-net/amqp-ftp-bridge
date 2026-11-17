package cli

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/vialabs-net/amqp-ftp-bridge/internal/config"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/creds"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/download"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/ftpup"
)

// cmdCheck reads every credential, connects to RabbitMQ (and checks every
// queue without consuming), logs in to every FTP target with every login the
// configuration uses, requests every source prefix and binds the HTTP
// listen address.
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

	failed := 0
	report := func(what, target string, err error, detail string) {
		status := "OK  "
		if err != nil {
			status, detail = "FAIL", err.Error()
			failed++
		}
		fmt.Fprintf(stdout, "%s %-7s %-40s %s\n", status, what, target, detail)
	}

	store := newStore(cfg.DataDir)
	secrets := map[string]creds.Credential{}
	for _, name := range cfg.Credentials() {
		c, err := store.Get(name)
		report("cred", name, err, "readable")
		if err == nil {
			secrets[name] = c
		}
	}
	// A check whose credential is unreadable is skipped: the cred line
	// already failed.
	have := func(name string) bool { _, ok := secrets[name]; return name == "" || ok }

	if len(cfg.Routes) > 0 && have(cfg.AMQP.Credential) {
		checkAMQP(cfg, secrets, *timeout, report)
	}
	for _, l := range ftpLogins(cfg) {
		if !have(l.credential) {
			continue
		}
		c := secrets[l.credential]
		ctx, cancel := context.WithTimeout(context.Background(), *timeout)
		err := (&ftpup.Client{Address: l.target.Address, User: c.User, Password: c.Password, Timeout: *timeout}).Check(ctx)
		cancel()
		name := l.target.Name + " " + l.target.Address
		if l.credential != l.target.Credential {
			name += " (" + l.credential + ")"
		}
		report("ftp", name, err, "login ok")
	}
	for _, s := range cfg.Sources {
		if !have(s.Credential) {
			continue
		}
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
	if cfg.HTTP.Enabled() {
		ln, err := net.Listen("tcp", cfg.HTTP.Listen)
		if err == nil {
			_ = ln.Close()
		}
		report("http", cfg.HTTP.Listen, err, fmt.Sprintf("can listen, %d route(s)", len(cfg.HTTP.Routes)))
	}
	if failed > 0 {
		fmt.Fprintf(stderr, "%d check(s) failed\n", failed)
		return errReported
	}
	return nil
}

type ftpLogin struct {
	target     config.FTP
	credential string
}

// ftpLogins returns each FTP target with its own credential, then the
// credentials that HTTP routes select per request, without duplicates.
func ftpLogins(cfg *config.Config) []ftpLogin {
	var out []ftpLogin
	seen := map[[2]string]bool{}
	add := func(t config.FTP, cred string) {
		k := [2]string{t.Name, cred}
		if !seen[k] {
			seen[k] = true
			out = append(out, ftpLogin{t, cred})
		}
	}
	for _, f := range cfg.FTP {
		add(f, f.Credential)
	}
	if cfg.HTTP.Enabled() {
		for _, r := range cfg.HTTP.Routes {
			t, _ := cfg.Target(r.FTP)
			values := make([]string, 0, len(r.FTPCredentials))
			for v := range r.FTPCredentials {
				values = append(values, v)
			}
			sort.Strings(values)
			for _, v := range values {
				add(t, r.FTPCredentials[v])
			}
		}
	}
	return out
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

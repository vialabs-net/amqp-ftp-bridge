package cli

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/vialabs-net/amqp-ftp-bridge/internal/audit"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/bridge"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/config"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/creds"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/download"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/ftpup"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/httpserver"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/metrics"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/oplog"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/pathpattern"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/tempfiles"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/version"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/winsvc"
)

func cmdRun(args []string, stderr io.Writer) error {
	fs, cfgPath := newFlags("run", stderr)
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	elog := openEvLog()
	defer elog.Close()

	if isService() {
		err := runService(func(ctx context.Context) error {
			return serve(ctx, *cfgPath, nil, elog)
		})
		if err != nil {
			elog.Error("amqp-ftp-bridge stopped with an error: " + err.Error())
		}
		return err
	}
	ctx, stop := notifyCtx()
	defer stop()
	if err := serve(ctx, *cfgPath, stderr, elog); err != nil {
		return err
	}
	return nil
}

// fatal reports err to the Event Log and returns it; Main prints it to stderr.
func fatal(elog winsvc.EventLog, err error) error {
	elog.Error("amqp-ftp-bridge cannot start: " + err.Error())
	return err
}

// serve runs the bridge until ctx is cancelled. mirror receives a copy of the
// operational log (stderr in the foreground).
func serve(ctx context.Context, cfgPath string, mirror io.Writer, elog winsvc.EventLog) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fatal(elog, err)
	}
	secrets, err := creds.LoadAll(newStore(cfg.DataDir), cfg.Credentials())
	if err != nil {
		return fatal(elog, err)
	}
	lease, err := tempfiles.Acquire(cfg.TempDir)
	if err != nil {
		return fatal(elog, fmt.Errorf("temporary directory: %w", err))
	}
	defer lease.Close()
	log, logFile, err := oplog.New(cfg.LogDir, mirror)
	if err != nil {
		return fatal(elog, err)
	}
	defer logFile.Close()

	hostname, _ := os.Hostname()
	aw, err := audit.NewWriter(cfg.LogDir, version.Version, hostname)
	if err != nil {
		return fatal(elog, err)
	}
	defer aw.Close()
	cleanup(log, cfg)

	// runCtx also ends when a component fails, so that the others stop.
	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()

	var obs bridge.Observer = bridge.NopObserver{}
	var httpObs httpserver.Observer
	if cfg.Metrics.Listen != "" {
		prom := metrics.New(version.Version, version.Commit)
		prom.SetAMQPRequired(len(cfg.Routes) > 0)
		addr, errc, err := prom.Serve(runCtx, cfg.Metrics.Listen)
		if err != nil {
			return fatal(elog, fmt.Errorf("metrics listener: %w", err))
		}
		go func() {
			if err := <-errc; err != nil {
				log.Error("metrics server stopped", "err", err)
			}
		}()
		log.Info("metrics listening", "addr", addr.String())
		obs, httpObs = prom, prom
	}

	var runners []func(context.Context) error
	if len(cfg.Routes) > 0 {
		b, err := build(cfg, secrets, aw, log, obs)
		if err != nil {
			return fatal(elog, err)
		}
		runners = append(runners, b.Run)
	}
	if cfg.HTTP.Enabled() {
		hs, err := buildHTTP(cfg, secrets, aw, log, httpObs)
		if err != nil {
			return fatal(elog, err)
		}
		ln, err := net.Listen("tcp", cfg.HTTP.Listen)
		if err != nil {
			return fatal(elog, fmt.Errorf("http listener: %w", err))
		}
		log.Info("http listening", "addr", ln.Addr().String(), "routes", len(cfg.HTTP.Routes))
		runners = append(runners, func(ctx context.Context) error {
			if err := hs.Serve(ctx, ln, cfg.Shutdown); err != nil {
				return fmt.Errorf("http server: %w", err)
			}
			return nil
		})
	}

	msg := fmt.Sprintf("amqp-ftp-bridge %s started with %d queue route(s) and %d http route(s)", version.Version, len(cfg.Routes), len(cfg.HTTP.Routes))
	log.Info(msg, "commit", version.Commit, "config", cfgPath)
	elog.Info(msg)

	dailyDone := make(chan struct{})
	go func() {
		defer close(dailyDone)
		daily(runCtx, log, aw, cfg)
	}()
	errs := make(chan error, len(runners))
	for _, r := range runners {
		go func() {
			err := r(runCtx)
			if err != nil {
				log.Error("component stopped", "err", err)
				stopRun()
			}
			errs <- err
		}()
	}
	var runErr error
	for range runners {
		runErr = errors.Join(runErr, <-errs)
	}
	stopRun()
	<-dailyDone
	log.Info("amqp-ftp-bridge stopped")
	elog.Info("amqp-ftp-bridge stopped")
	return runErr
}

func buildHTTP(cfg *config.Config, secrets map[string]creds.Credential, aw *audit.Writer, log *slog.Logger, obs httpserver.Observer) (*httpserver.Server, error) {
	h := cfg.HTTP
	s := &httpserver.Server{
		MaxUploadBytes:    int64(h.MaxUploadBytes),
		TempDir:           cfg.TempDir,
		Sessions:          httpserver.NewSessions(h.MaxFTPSessions, obs),
		FTPWait:           h.FTPWait,
		Audit:             aw,
		Log:               log,
		Obs:               obs,
		ReadHeaderTimeout: h.ReadHeaderTimeout,
		ReadTimeout:       h.ReadTimeout,
		WriteTimeout:      h.WriteTimeout,
		IdleTimeout:       h.IdleTimeout,
	}
	for _, name := range h.Credentials {
		s.Users = append(s.Users, secrets[name])
	}
	for _, p := range h.TrustedProxies {
		prefix, err := config.ParseTrustedProxy(p)
		if err != nil {
			return nil, err
		}
		s.TrustedProxies = append(s.TrustedProxies, prefix)
	}
	for _, r := range h.Routes {
		pattern, err := pathpattern.Compile(r.Path)
		if err != nil {
			return nil, err
		}
		tgt, _ := cfg.Target(r.FTP)
		fc := secrets[tgt.Credential]
		route := httpserver.Route{
			Name: r.Name, Action: r.Action, Method: r.Method, Pattern: pattern,
			Folder: r.Folder, FileName: r.FileName, CredentialBy: r.FTPCredentialBy,
			Target: httpserver.Target{Name: tgt.Name, Address: tgt.Address, User: fc.User, Password: fc.Password, Timeout: tgt.Timeout},
		}
		if len(r.FTPCredentials) > 0 {
			route.Credentials = map[string]creds.Credential{}
			for value, name := range r.FTPCredentials {
				route.Credentials[value] = secrets[name]
			}
		}
		s.Routes = append(s.Routes, route)
	}
	return s, nil
}

func build(cfg *config.Config, secrets map[string]creds.Credential, aw *audit.Writer, log *slog.Logger, obs bridge.Observer) (*bridge.Bridge, error) {
	u, err := url.Parse(cfg.AMQP.URL)
	if err != nil {
		return nil, err
	}
	c := secrets[cfg.AMQP.Credential]
	u.User = url.UserPassword(c.User, c.Password)

	b := &bridge.Bridge{
		URL:               u.String(),
		Heartbeat:         cfg.AMQP.Heartbeat,
		ReconnectMax:      cfg.AMQP.ReconnectMax,
		DeclareDeadLetter: *cfg.DeadLetter.Declare,
		ShutdownTimeout:   cfg.Shutdown,
		Log:               log,
		Obs:               obs,
	}
	for _, r := range cfg.Routes {
		src, _ := cfg.Source(r.Source)
		tgt, _ := cfg.Target(r.Target)
		sc := secrets[src.Credential]
		fc := secrets[tgt.Credential]
		b.Processors = append(b.Processors, &bridge.Processor{
			Route: bridge.Route{
				Queue:              r.Queue,
				SourceName:         src.Name,
				DeadLetterExchange: r.DeadLetter.Exchange,
				DeadLetterKey:      r.DeadLetter.RoutingKey,
				Mapping:            r.Mapping,
				Match:              r.Match,
				Downloader:         download.New(src.URLPrefix, sc.User, sc.Password, cfg.TempDir, src.Timeout),
				Uploader:           &ftpup.Client{Address: tgt.Address, User: fc.User, Password: fc.Password, Timeout: tgt.Timeout, RecoveryDir: filepath.Join(cfg.TempDir, fmt.Sprintf("ftp-%x", sha256.Sum256([]byte(r.Queue+"\x00"+tgt.Address+"\x00"+fc.User))))},
				TempDir:            cfg.TempDir,
			},
			Audit:            aw,
			Log:              log,
			Obs:              obs,
			ErrorHeader:      cfg.DeadLetter.ErrorHeader,
			StacktraceHeader: cfg.DeadLetter.StacktraceHeader,
			RequeueDelay:     5 * time.Second,
		})
	}
	return b, nil
}

func cleanup(log *slog.Logger, cfg *config.Config) {
	deleted, err := audit.Cleanup(cfg.LogDir, cfg.Audit.RetentionDays, time.Now())
	if err != nil {
		log.Warn("log retention cleanup failed", "err", err)
	}
	if len(deleted) > 0 {
		log.Info("old log files deleted", "count", len(deleted), "retention_days", cfg.Audit.RetentionDays)
	}
}

// daily writes the daily summary and applies retention at each local midnight.
func daily(ctx context.Context, log *slog.Logger, aw *audit.Writer, cfg *config.Config) {
	for {
		wait := time.Until(audit.NextMidnight(time.Now()))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		if err := aw.Rollover(); err != nil {
			log.Error("daily summary failed", "err", err)
		}
		cleanup(log, cfg)
	}
}

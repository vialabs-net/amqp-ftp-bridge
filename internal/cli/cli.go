// Package cli implements the amqp-ftp-bridge commands.
package cli

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/term"

	"github.com/vialabs-net/amqp-ftp-bridge/internal/audit"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/config"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/creds"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/version"
	"github.com/vialabs-net/amqp-ftp-bridge/internal/winsvc"
)

const usage = `Usage: amqp-ftp-bridge <command> [flags]

Commands:
  run                  run in the foreground (or as the Windows service)
  service install      install the Windows service (automatic start)
  service uninstall    remove the Windows service
  service start        start the Windows service
  service stop         stop the Windows service
  service grant-logon  grant an existing --account the Windows service logon right
  creds set <name>     store a credential (prompts for user and password)
  creds secure         migrate Windows credential ACLs (administrator only)
  check                test the connections to RabbitMQ, FTP and every source
  audit                print audit records (--date, --queue, --file, --result)
  version              print the version

Common flags:
  --config <path>      configuration file (default: %s)
`

// Hooks replaced by tests.
var (
	newStore    = creds.Default
	secureCreds = creds.Secure
	isService   = winsvc.IsService
	runService  = winsvc.RunService
	openEvLog   = winsvc.OpenEventLog
	svcInstall  = winsvc.Install
	svcRemove   = winsvc.Uninstall
	svcStart    = winsvc.Start
	svcStop     = winsvc.Stop
	executable  = os.Executable
	notifyCtx   = func() (context.Context, context.CancelFunc) {
		return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	}
	readPasswd  = term.ReadPassword
	isTerminal  = term.IsTerminal
	errUsage    = errors.New("usage")
	errBadFlag  = errors.New("bad flag")
	errReported = errors.New("reported")
)

// Main runs the command in args and returns the process exit code.
func Main(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	err := dispatch(args, stdin, stdout, stderr)
	switch {
	case err == nil:
		return 0
	case errors.Is(err, errUsage):
		fmt.Fprintf(stderr, usage, config.DefaultPath())
		return 2
	case errors.Is(err, flag.ErrHelp), errors.Is(err, errBadFlag):
		return 2
	case errors.Is(err, errReported):
		return 1
	}
	fmt.Fprintln(stderr, "error:", err)
	return 1
}

func dispatch(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errUsage
	}
	cmd, rest := args[0], args[1:]
	switch cmd {
	case "run":
		return cmdRun(rest, stderr)
	case "service":
		return cmdService(rest, stdout, stderr)
	case "creds":
		return cmdCreds(rest, stdin, stdout, stderr)
	case "check":
		return cmdCheck(rest, stdout, stderr)
	case "audit":
		return cmdAudit(rest, stdout, stderr)
	case "version", "--version", "-v":
		fmt.Fprintf(stdout, "amqp-ftp-bridge %s (%s)\n", version.Version, version.Commit)
		return nil
	case "help", "--help", "-h":
		fmt.Fprintf(stdout, usage, config.DefaultPath())
		return nil
	}
	fmt.Fprintf(stderr, "unknown command %q\n", cmd)
	return errUsage
}

func newFlags(name string, stderr io.Writer) (*flag.FlagSet, *string) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfg := fs.String("config", config.DefaultPath(), "configuration file")
	return fs, cfg
}

// parseFlags parses args; the flag package has already printed any error.
func parseFlags(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return err
		}
		return errBadFlag
	}
	return nil
}

func cmdService(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errUsage
	}
	fs, cfgPath := newFlags("service "+args[0], stderr)
	account := fs.String("account", "", "existing service account")
	if err := parseFlags(fs, args[1:]); err != nil {
		return err
	}
	switch args[0] {
	case "grant-logon":
		if *account == "" || fs.NArg() != 0 {
			return errUsage
		}
		return winsvc.GrantServiceLogon(*account)
	case "install":
		abs, err := filepath.Abs(*cfgPath)
		if err != nil {
			return err
		}
		if _, err := config.Load(abs); err != nil {
			return err
		}
		exe, err := executable()
		if err != nil {
			return err
		}
		if err := svcInstall(exe, abs); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "service %s installed (config %s)\n", winsvc.Name, abs)
	case "uninstall":
		if err := svcRemove(); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "service %s removed\n", winsvc.Name)
	case "start":
		if err := svcStart(); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "service %s started\n", winsvc.Name)
	case "stop":
		if err := svcStop(); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "service %s stopped\n", winsvc.Name)
	default:
		return errUsage
	}
	return nil
}

func cmdCreds(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) == 0 || (args[0] != "set" && args[0] != "secure") {
		return errUsage
	}
	fs, cfgPath := newFlags("creds "+args[0], stderr)
	dataDir := fs.String("data-dir", "", "data directory (default: data_dir from the configuration)")
	if err := parseFlags(fs, args[1:]); err != nil {
		return err
	}
	if args[0] == "secure" {
		if fs.NArg() != 0 || *dataDir == "" {
			return errUsage
		}
		return secureCreds(*dataDir)
	}
	if fs.NArg() != 1 {
		return errUsage
	}
	name := fs.Arg(0)
	if err := creds.ValidName(name); err != nil {
		return err
	}
	dir := *dataDir
	if dir == "" {
		dir = config.DefaultDataDir()
		if cfg, err := config.Load(*cfgPath); err == nil {
			dir = cfg.DataDir
		}
	}
	in := bufio.NewReader(stdin)
	fmt.Fprint(stdout, "User: ")
	user, err := in.ReadString('\n')
	if err != nil && !(errors.Is(err, io.EOF) && user != "") {
		return fmt.Errorf("read user: %w", err)
	}
	user = strings.TrimRight(user, "\r\n")
	fmt.Fprint(stdout, "Password: ")
	pass, err := readSecret(stdin, in)
	fmt.Fprintln(stdout)
	if err != nil {
		return fmt.Errorf("read password: %w", err)
	}
	if user == "" || pass == "" {
		return errors.New("user and password must not be empty")
	}
	if err := newStore(dir).Set(name, creds.Credential{User: user, Password: pass}); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "credential %q saved\n", name)
	return nil
}

// readSecret reads without echo when stdin is a terminal, else one line.
func readSecret(stdin io.Reader, buffered *bufio.Reader) (string, error) {
	if f, ok := stdin.(*os.File); ok && isTerminal(int(f.Fd())) {
		b, err := readPasswd(int(f.Fd()))
		return string(b), err
	}
	s, err := buffered.ReadString('\n')
	if err != nil && !(errors.Is(err, io.EOF) && s != "") {
		return "", err
	}
	return strings.TrimRight(s, "\r\n"), nil
}

func cmdAudit(args []string, stdout, stderr io.Writer) error {
	fs, cfgPath := newFlags("audit", stderr)
	logDir := fs.String("log-dir", "", "audit directory (default: log_dir from the configuration)")
	var f audit.Filter
	fs.StringVar(&f.Date, "date", "", "day to read, YYYY-MM-DD (default: today)")
	fs.StringVar(&f.Queue, "queue", "", "queue name")
	fs.StringVar(&f.File, "file", "", "file name pattern, for example '*.pdf'")
	fs.StringVar(&f.Result, "result", "", "acked, dead_lettered or incomplete")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	dir := *logDir
	if dir == "" {
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		dir = cfg.LogDir
	}
	n, err := audit.Query(dir, f, stdout)
	if err != nil {
		return err
	}
	fmt.Fprintf(stderr, "%d message(s)\n", n)
	return nil
}

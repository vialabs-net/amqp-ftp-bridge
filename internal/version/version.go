// Package version holds build information set with -ldflags.
package version

// Version and Commit are set at build time:
//
//	go build -ldflags "-X github.com/vialabs-net/amqp-ftp-bridge/internal/version.Version=1.2.3 -X github.com/vialabs-net/amqp-ftp-bridge/internal/version.Commit=abc123"
var (
	Version = "dev"
	Commit  = "unknown"
)

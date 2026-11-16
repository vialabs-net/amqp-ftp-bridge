// Command amqp-ftp-bridge consumes RabbitMQ queues, downloads the file each
// message points to over HTTP and uploads it to an FTP server.
package main

import (
	"os"

	"github.com/vialabs-net/amqp-ftp-bridge/internal/cli"
)

func main() {
	os.Exit(cli.Main(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

# amqp-ftp-bridge

A Windows service that consumes RabbitMQ (AMQP 0-9-1) queues, downloads the file each message points to over HTTP, and uploads it to an FTP server.

Status: in design. No release yet.

| Feature | Detail |
|---|---|
| Input | RabbitMQ queues, one message at a time, acknowledged after the upload |
| Download | HTTP(S) with Basic authentication |
| Output | FTP, passive mode, binary, creates missing folders |
| Errors | The original message goes to a dead-letter exchange with the error in configurable headers |
| Audit | One JSON line per step and message, with the file's SHA-256 |
| Metrics | Optional Prometheus endpoint or OTLP push |
| Credentials | Encrypted with Windows DPAPI (machine scope) |
| Install | Signed MSI |

The message mapping (which JSON field or AMQP header gives the source, folder and file name) is set in the configuration, so the service carries no knowledge of any specific system.

## License

Apache 2.0. See [LICENSE](LICENSE).

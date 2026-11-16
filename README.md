# amqp-ftp-bridge

A Windows service that consumes RabbitMQ (AMQP 0-9-1) queues, downloads the file each message points to over HTTP, and uploads it to an FTP server.

Each route links one queue to one download source and one FTP target. The message mapping is set in the configuration, so the service has no knowledge of any specific system.

## Behaviour

For each message, with `prefetch=1` and manual acknowledgement:

| Step | What happens |
|---|---|
| Consume | From an existing durable queue. The service does not declare or bind the queue. |
| Parse | Reads `source_id`, `folder` and `file_name` from the JSON body or AMQP headers, as mapped. |
| Match | Optional. Processes the message only when a field equals a value. Otherwise: `dead_letter` (default) or `upload_field`. |
| Download | `GET <url_prefix><source_id>` with HTTP Basic auth, streamed to a temp file. Non-2xx is a failure. |
| Upload | FTP, passive mode, binary. Creates missing folders and overwrites `<folder>/<file_name>`. |
| Ack | Acknowledges the message and deletes the temp file. |

`upload_field` uploads the JSON of the given field as the file, to the mapped `folder` and `file_name`, without downloading.

`source_id` is URL path-escaped. A `folder` or `file_name` containing `..`, and a `file_name` containing `/` or `\`, are failures.

**On any failure** the service publishes the original body to the dead-letter exchange, with the original headers and properties plus two headers:

| Header (default) | Value |
|---|---|
| `x-error` | Error message |
| `x-error-stacktrace` | Failed step, HTTP or FTP code, and the chain of wrapped errors |

The dead-letter exchange is `<queue>_deadLetter` (direct, durable) with routing key `<queue>_deadLetter`. The original message is acked only after the broker confirms the dead-letter publish. If that publish fails, the message is requeued after 5 seconds. There are no retries.

Error diagnostics are limited to 2 KiB each and to the available AMQP header frame space. If the original headers leave no space, diagnostics are omitted. The original producer's `user_id` is omitted because RabbitMQ validates it against the publishing account.

Dead-letter messages have no active expiration; the original TTL is recorded in `x-original-expiration`. FTP uploads use a temporary remote file and rename it to the destination only after a successful transfer. A failed transfer preserves the existing destination.

If the process stops during a message, RabbitMQ redelivers it and the upload overwrites the file. At startup, the service locks its temporary directory and removes abandoned download and field files. A second instance sharing that directory fails to start until the first releases it.

The service reconnects to RabbitMQ with exponential backoff (1 s up to `reconnect_max`, default 60 s). On stop it cancels the consumers, waits for the in-flight messages (up to `shutdown_timeout`) and exits.

## Configuration

YAML. Default path: `%ProgramData%\amqp-ftp-bridge\config.yaml`. Override with `--config`. See [config.example.yaml](config.example.yaml) for a commented example.

| Key | Default | Purpose |
|---|---|---|
| `data_dir` | `%ProgramData%\amqp-ftp-bridge` | Credentials and default directories |
| `log_dir` | `<data_dir>\logs` | Audit and service logs |
| `temp_dir` | `<data_dir>\tmp` | Downloads before upload |
| `shutdown_timeout` | `5m` | Wait for in-flight messages on stop |
| `audit.retention_days` | `365` | Log files older than this are deleted |
| `amqp.url` | required | `amqp://` or `amqps://` URL without credentials |
| `amqp.credential` | required | Credential name for RabbitMQ |
| `amqp.heartbeat` | `10s` | AMQP heartbeat |
| `amqp.reconnect_max` | `60s` | Longest reconnection wait |
| `dead_letter.exchange_suffix` | `_deadLetter` | Dead-letter exchange is `<queue><suffix>` |
| `dead_letter.routing_key_suffix` | `_deadLetter` | Routing key is `<queue><suffix>` |
| `dead_letter.error_header` | `x-error` | Error message header |
| `dead_letter.stacktrace_header` | `x-error-stacktrace` | Error chain header |
| `dead_letter.declare` | `true` | Declare the exchange, a durable queue named after the routing key, and the binding |
| `metrics.listen` | empty (off) | Address for `/metrics`, `/healthz`, `/readyz` |
| `sources[].name` | required | Source name used by routes |
| `sources[].url_prefix` | required | HTTP(S) prefix; the id is appended |
| `sources[].credential` | none | Credential for HTTP Basic auth |
| `sources[].timeout` | `5m` | Timeout for one download |
| `ftp[].name` | required | Target name used by routes |
| `ftp[].address` | required | `host:port` |
| `ftp[].credential` | required | Credential for the FTP login |
| `ftp[].timeout` | `1m` | Timeout for each FTP operation |
| `routes[].queue` | required | Queue to consume |
| `routes[].source` | required | Source name |
| `routes[].target` | required | FTP target name |
| `routes[].mapping.source_id` | required | Selector for the download id |
| `routes[].mapping.folder` | required | Selector for the FTP folder |
| `routes[].mapping.file_name` | required | Selector for the FTP file name |
| `routes[].match.field` | none | Selector compared with `equals` |
| `routes[].match.equals` | none | Value that must match |
| `routes[].match.otherwise` | `dead_letter` | `dead_letter` or `upload_field` |
| `routes[].match.upload_field` | none | JSON path uploaded when `otherwise: upload_field` |
| `routes[].dead_letter.exchange` | `<queue>_deadLetter` | Per-route exchange |
| `routes[].dead_letter.routing_key` | `<queue>_deadLetter` | Per-route routing key |

**Selectors**

| Form | Reads |
|---|---|
| `data._file.id` | JSON path in the body. Numeric parts index arrays: `items.0.id`. |
| `header:DestinationPath` | AMQP header. Exact name first, then case-insensitive. |
| `literal:inbox` | Fixed value |

## Audit log

JSON Lines, one file per local day: `<log_dir>/audit-YYYY-MM-DD.jsonl`. One record per step. All records of a message share `msg_id`: the AMQP `message-id`, or a generated UUID.

Every record has `ts` (RFC 3339, milliseconds, local offset), `step`, `msg_id`, `queue`, `version` and `hostname`.

| `step` | Fields |
|---|---|
| `received` | `event`, `matched` (when a match rule is set), `body` (or `body_text` when not JSON), `redelivered` |
| `downloaded` | `source`, `source_id`, `host`, `http_status`, `bytes`, `sha256`, `ms` |
| `uploaded` | `folder`, `file_name`, `ftp_code`, `bytes`, `ms` |
| `acked` | `total_ms` |
| `dead_lettered` | `failed_step`, `error`, `http_status` or `ftp_code` when known, `folder`, `file_name` when known |
| `daily_summary` | `date`, `queues`: per queue `received`, `uploaded`, `dead_lettered`, `bytes` |

`daily_summary` is written at local midnight to the file of the day that ended. At startup, today's totals are rebuilt from today's file, and a missing summary for yesterday is written.

The log never contains credentials, `Authorization` headers or file contents. `host` never contains user information.

Retention: files older than `audit.retention_days` are deleted at startup and every midnight. This applies to `audit-*.jsonl` and `service-*.log`.

Example:

```json
{"ts":"2026-03-01T10:15:02.317-03:00","step":"downloaded","msg_id":"0b6c…","queue":"files.upload","version":"1.0.0","hostname":"SRV01","source":"files-api","source_id":"F-100","host":"files.example.com","http_status":200,"sha256":"9f2…","bytes":48213,"ms":184}
```

**Other logs**

| Log | Content |
|---|---|
| `<log_dir>/service-YYYY-MM-DD.log` | Start, stop, connect, reconnect, warnings. Also printed to stderr by `run` in the foreground. |
| Windows Event Log, source `amqp-ftp-bridge` | Start, stop, fatal errors |

## Metrics

Off by default. Set `metrics.listen` (for example `0.0.0.0:9464`).

| Metric | Labels |
|---|---|
| `bridge_messages_total` | `queue`, `result` (`uploaded`, `dead_lettered`, `requeued`) |
| `bridge_redelivered_total` | `queue` |
| `bridge_stage_duration_seconds` | `queue`, `stage` (`download`, `upload`, `total`) |
| `bridge_bytes_total` | `queue`, `stage` (`download`, `upload`) |
| `bridge_errors_total` | `queue`, `stage`, `code` (HTTP or FTP code, `0` when none) |
| `bridge_inflight` | `queue` |
| `bridge_last_success_timestamp_seconds` | `queue` |
| `bridge_amqp_connected` | |
| `bridge_amqp_reconnects_total` | |
| `bridge_build_info` | `version`, `commit` |

The Go and process collectors are included.

| Path | Answer |
|---|---|
| `/healthz` | 200 while the process runs |
| `/readyz` | 200 when connected to RabbitMQ and the last FTP upload succeeded, else 503 |

Other metrics backends implement the `bridge.Observer` interface in `internal/bridge/observer.go`.

## Credentials

The configuration holds credential names only (`credential: rabbitmq`). Store each one with:

```
amqp-ftp-bridge creds set rabbitmq
```

It asks for the user and the password (without echo).

| Platform | Storage |
|---|---|
| Windows | `<data_dir>\credentials\<name>.bin`, encrypted with DPAPI, machine scope. Directory and file ACL: SYSTEM and Administrators only. |
| Other (development and tests only) | Environment variable `AFB_CRED_<NAME>=user:password`. `NAME` is upper case, other characters become `_`. |

If a credential cannot be read, the service does not start. It writes the reason to stderr and the Event Log.

## CLI

```
amqp-ftp-bridge <command> [--config <path>]
```

| Command | Action |
|---|---|
| `run` | Run in the foreground. Under the service manager, run as the service. |
| `service install` | Install the Windows service: automatic start, restart 10 s after a failure. Stores the absolute `--config` path. |
| `service uninstall` | Stop and remove the service |
| `service start` / `service stop` | Start or stop the service |
| `creds set <name>` | Store a credential. `--data-dir` overrides `data_dir`. |
| `check` | Connect to RabbitMQ and check each queue exists (passive), log in to each FTP target, request each source prefix (HEAD, then GET). Consumes nothing. Exit code 1 if any check fails. |
| `audit` | Print the records of matching messages. Flags: `--date YYYY-MM-DD` (default today), `--queue`, `--file` (glob on the file name), `--result` (`acked`, `dead_lettered`, `incomplete`), `--log-dir`. |
| `version` | Print version and commit |

## Build and test

```
go build ./cmd/amqp-ftp-bridge
GOOS=windows go build ./cmd/amqp-ftp-bridge
go test ./...
go test -tags integration ./...   # needs Docker
```

The integration tests start RabbitMQ and an FTP server with testcontainers.

## License

Apache 2.0. See [LICENSE](LICENSE).

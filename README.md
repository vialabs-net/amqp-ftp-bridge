# amqp-ftp-bridge

[![CI](https://github.com/vialabs-net/amqp-ftp-bridge/actions/workflows/ci.yml/badge.svg?branch=trunk)](https://github.com/vialabs-net/amqp-ftp-bridge/actions/workflows/ci.yml?query=branch%3Atrunk) [![Quality Gate](https://sonarcloud.io/api/project_badges/measure?project=vialabs-net_amqp-ftp-bridge&metric=alert_status)](https://sonarcloud.io/summary/overall?id=vialabs-net_amqp-ftp-bridge) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=vialabs-net_amqp-ftp-bridge&metric=coverage)](https://sonarcloud.io/summary/overall?id=vialabs-net_amqp-ftp-bridge) [![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=vialabs-net_amqp-ftp-bridge&metric=security_rating)](https://sonarcloud.io/summary/overall?id=vialabs-net_amqp-ftp-bridge) [![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=vialabs-net_amqp-ftp-bridge&metric=reliability_rating)](https://sonarcloud.io/summary/overall?id=vialabs-net_amqp-ftp-bridge) [![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=vialabs-net_amqp-ftp-bridge&metric=sqale_rating)](https://sonarcloud.io/summary/overall?id=vialabs-net_amqp-ftp-bridge) [![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=vialabs-net_amqp-ftp-bridge&metric=vulnerabilities)](https://sonarcloud.io/summary/overall?id=vialabs-net_amqp-ftp-bridge) [![Bugs](https://sonarcloud.io/api/project_badges/measure?project=vialabs-net_amqp-ftp-bridge&metric=bugs)](https://sonarcloud.io/summary/overall?id=vialabs-net_amqp-ftp-bridge) [![Release](https://img.shields.io/github/v/release/vialabs-net/amqp-ftp-bridge)](https://github.com/vialabs-net/amqp-ftp-bridge/releases) [![Go](https://img.shields.io/github/go-mod/go-version/vialabs-net/amqp-ftp-bridge)](go.mod) [![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

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

Dead-letter messages have no active expiration; the original TTL is recorded in `x-original-expiration` when frame space permits. FTP uploads use a temporary remote file and rename it to the destination only after a successful transfer. A failed transfer preserves the existing destination. Per-route journals under the locked temporary directory record remote staging names before STOR. Failed transfers clean up through a fresh FTP connection; the next upload recovers recorded staging files after a disconnect or process restart.

If the process stops during a message, RabbitMQ redelivers it and the upload overwrites the file. On Windows, temporary contents inherit a protected directory ACL: SYSTEM, Administrators and the service SID only. At startup, the service locks its temporary directory and removes abandoned download and field files. A second instance sharing that directory fails to start until the first releases it.

The service reconnects to RabbitMQ with exponential backoff (1 s up to `reconnect_max`, default 60 s). On stop it cancels the consumers, waits for the in-flight messages (up to `shutdown_timeout`) and exits.

## Install

Download `amqp-ftp-bridge.msi` from the [releases](https://github.com/vialabs-net/amqp-ftp-bridge/releases). Run it as an administrator.

| Path | Content |
|---|---|
| `C:\Program Files\amqp-ftp-bridge\` | `amqp-ftp-bridge.exe`, `config.example.yaml` |
| `%ProgramData%\amqp-ftp-bridge\` | `config.yaml`, `logs\`, `credentials\`, `tmp\` |

The data directories do not inherit permissions. SYSTEM and Administrators have full control. The service SID `NT SERVICE\amqp-ftp-bridge` can read the configuration and credentials and write to `logs` and `tmp`. Nobody else has access.

The service `amqp-ftp-bridge` ("AMQP FTP Bridge") starts automatically and restarts 10 s after a failure. The install starts it only when `config.yaml` exists or `CONFIG_FILE` is given.

**MSI properties**

| Property | Default | Purpose |
|---|---|---|
| `SERVICE_ACCOUNT` | `NT SERVICE\amqp-ftp-bridge` | Service account: the virtual account, a gMSA (`DOMAIN\name$`) or a user (`DOMAIN\user`) |
| `SERVICE_PASSWORD` | empty | Password of a user account. Not stored. |
| `CONFIG_FILE` | none | Full path of a configuration copied to `%ProgramData%\amqp-ftp-bridge\config.yaml`, replacing the existing one |
| `METRICS_ALLOW_FROM` | none | Adds an inbound firewall rule for TCP `METRICS_PORT` from this address only (`192.0.2.10`, or a list: `192.0.2.10,192.0.2.0/24`). No rule when empty. |
| `METRICS_PORT` | `9464` | Port of the firewall rule. Must match `metrics.listen`. |

`SERVICE_ACCOUNT`, `METRICS_ALLOW_FROM` and `METRICS_PORT` are stored in `HKLM\SOFTWARE\vialabs\amqp-ftp-bridge`. An upgrade that does not pass them uses the stored values.

For a custom user or gMSA, provision the account on the machine before installing. The MSI grants it [Log on as a service](https://learn.microsoft.com/en-us/windows/win32/msi/serviceinstall-table); domain policies that deny service logon still take precedence. The MSI does not create accounts. A gMSA uses an empty `SERVICE_PASSWORD`. The logon right is retained on uninstall because other services may use the account.

**Silent install**

```
msiexec /i amqp-ftp-bridge.msi /qn /l*v install.log CONFIG_FILE=C:\setup\config.yaml METRICS_ALLOW_FROM=192.0.2.10
"C:\Program Files\amqp-ftp-bridge\amqp-ftp-bridge.exe" creds set rabbitmq
"C:\Program Files\amqp-ftp-bridge\amqp-ftp-bridge.exe" service start
```

Store every credential the configuration names, then start the service. Until then the service stops at startup and restarts every 10 s.

**Upgrade**

Install the new MSI the same way. The upgrade replaces the program and keeps the configuration, credentials and logs. It also grants the service SID read access to existing encrypted credentials without rewriting them. A failed upgrade rolls back the product replacement. With a user account, pass `SERVICE_PASSWORD` again; after a rollback, you may need to restore the service password in Services because Windows Installer cannot retrieve it.

**Uninstall**

```
msiexec /x amqp-ftp-bridge.msi /qn
```

Or remove "amqp-ftp-bridge" in Settings, Apps. Uninstall removes the program, the service, the Event Log source and the firewall rule. `%ProgramData%\amqp-ftp-bridge` stays: delete it to remove the configuration, credentials and logs.

Remove a service installed with `amqp-ftp-bridge service install` (`service uninstall`) before installing the MSI.

**Verify a download**

```
sha256sum -c SHA256SUMS --ignore-missing
gh attestation verify amqp-ftp-bridge.msi --repo vialabs-net/amqp-ftp-bridge
gh attestation verify amqp-ftp-bridge.exe --repo vialabs-net/amqp-ftp-bridge
```

In PowerShell, compare `Get-FileHash amqp-ftp-bridge.msi` with the line in `SHA256SUMS`. `gh attestation verify` checks that the file was built by this repository's release workflow from the tagged commit. Each release also has an SPDX SBOM, `amqp-ftp-bridge.spdx.json`.

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
| Windows | `<data_dir>\credentials\<name>.bin`, encrypted with DPAPI, machine scope. Directory and file ACL: SYSTEM and Administrators have full control; the service SID has read access. |
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
| `creds secure --data-dir <path>` | Migrate existing Windows credential ACLs to allow the service SID to read them. Requires an administrator; run automatically by the MSI. |
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

**Windows executable and MSI**

```
scripts/build-windows.sh 1.2.3 "$(git rev-parse --short=12 HEAD)" dist/amqp-ftp-bridge.exe
dotnet tool install --global wix --version 5.0.2
wix extension add --global WixToolset.Firewall.wixext/5.0.2 WixToolset.Util.wixext/5.0.2
installer/build.ps1 -Version 1.2.3 -Exe dist/amqp-ftp-bridge.exe -Out dist/amqp-ftp-bridge.msi
```

`build-windows.sh` adds the version resource (go-winres, `cmd/amqp-ftp-bridge/winres.json`). The MSI builds on Windows. `installer/smoke-test.ps1` installs, upgrades, starts and uninstalls on a disposable machine; CI runs it on every pull request.

## Release

Push a tag `v1.2.3`, or `v1.2.3-rc.1` for a pre-release. The workflow `release.yml` publishes a GitHub release with:

| File | Content |
|---|---|
| `amqp-ftp-bridge.exe` | Executable |
| `amqp-ftp-bridge.msi` | Installer |
| `amqp-ftp-bridge.spdx.json` | SPDX SBOM of the modules in the executable |
| `SHA256SUMS` | SHA-256 of the files above |

The exe, the MSI and the SBOM have build provenance attestations.

## Code signing

Releases are unsigned until signing is configured. The workflow signs the exe, builds the MSI with it, then signs the MSI.

**Azure Artifact Signing** (formerly Trusted Signing). Implemented. Runs when the repository variable `AZURE_SIGNING_ACCOUNT` is set.

1. Create an Artifact Signing account, complete identity validation and create a public-trust certificate profile.
2. Create an Entra app registration with a [flexible federated credential](https://learn.microsoft.com/en-us/entra/workload-id/workload-identities-set-up-flexible-federated-identity-credential) for this repository. Use issuer `https://token.actions.githubusercontent.com`, audience `api://AzureADTokenExchange` and `claimsMatchingExpression` with `languageVersion: 1` and value `claims['sub'] matches 'repo:vialabs-net/amqp-ftp-bridge:ref:refs/tags:v*' and claims['repository_id'] eq '<REPOSITORY_ID>'`. Get the numeric ID with `gh api repos/vialabs-net/amqp-ftp-bridge --jq .id`; leave `subject` unset. Standard "Tag" credentials match one exact tag and do not support wildcards. Give the app the role "Artifact Signing Certificate Profile Signer" on the profile.
3. Set these repository variables. No secret is needed: login uses OpenID Connect.

| Variable | Value |
|---|---|
| `AZURE_TENANT_ID` | Tenant ID |
| `AZURE_CLIENT_ID` | App registration client ID |
| `AZURE_SIGNING_ENDPOINT` | Account endpoint URL |
| `AZURE_SIGNING_ACCOUNT` | Account name |
| `AZURE_SIGNING_PROFILE` | Certificate profile name |

The workflow then fails unless both files have a valid Authenticode signature.

**SignPath** (not implemented). Free for open source projects through SignPath Foundation. Install the SignPath GitHub app, store `SIGNPATH_API_TOKEN` as a secret, and replace each signing step with `signpath/github-action-submit-signing-request`: upload the file as an artifact, submit it, and download the signed file. Sign the exe before building the MSI, then sign the MSI.

## License

Apache 2.0. See [LICENSE](LICENSE).

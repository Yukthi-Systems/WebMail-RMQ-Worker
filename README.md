# WebMail RMQ Worker

> A production-ready, concurrent RabbitMQ worker written in Go for delivering and drafting emails via SMTP and IMAP.

[![Go Version](https://img.shields.io/badge/Go-1.23%2B-00ADD8?logo=go)](https://golang.org/)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running the Application](#running-the-application)
  - [Local (Go)](#local-go)
  - [Docker](#docker)
  - [Docker Compose](#docker-compose)
- [Queue Message Payload](#queue-message-payload)
- [Project Structure](#project-structure)
- [Third-Party Libraries](#third-party-libraries)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

**WebMail RMQ Worker** is a background service that listens to a RabbitMQ queue and processes email tasks. Each message on the queue describes either:

- **Send** — deliver the email via SMTP and store a copy in the Sent IMAP folder.
- **Draft** — save (or update) the email in a designated IMAP Draft folder without sending it.

The worker is designed for high-throughput environments and uses connection pooling for both SMTP and IMAP to minimise per-message latency.

---

## Features

- **Email Sending** — delivers messages via SMTP with support for TLS, STARTTLS, and plain connections; auto-detects the correct security mode from the port number.
- **Draft Management** — atomically replaces an existing draft (search by `Message-ID` → delete → append) so the user always sees a single up-to-date draft.
- **Connection Pooling** — maintains reusable pools of authenticated SMTP and IMAP connections; falls back to temporary connections under load.
- **Dead-Letter Queue** — failed messages are published to a configurable DLQ with an `x-dead-letter-reason` header for later inspection.
- **Concurrent Workers** — a configurable number of goroutine workers consume from the same queue channel, enabling parallel processing.
- **Structured Logging** — JSON-structured logs via zerolog with automatic file rotation (lumberjack); optional console output for development.
- **Graceful Shutdown** — handles `SIGINT`/`SIGTERM` and drains in-flight messages before exiting.
- **Environment-Based Config** — all settings are read from environment variables; a `.env` file is supported for local development.

---

## Architecture

```
RabbitMQ (main queue)
        │
        ▼
  ┌─────────────┐
  │  Consumer   │  (goroutine worker pool)
  └──────┬──────┘
         │  unmarshal JSON payload
         ▼
  ┌─────────────────┐
  │  EmailService   │
  │  ProcessMail()  │
  └──────┬──────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
 IsDraft?   Send mode
    │           │
    │       SMTPSenderPool ──► SMTP Server
    │           │
    └─────► IMAPPool ──────► IMAP Server
              (Append / Search / Delete)

On failure:
  Consumer ──► RabbitMQ (dead-letter queue)
```

---

## Prerequisites

| Requirement | Version |
|---|---|
| [Go](https://golang.org/dl/) | 1.23 or higher |
| [RabbitMQ](https://www.rabbitmq.com/) | 3.x or higher |
| An SMTP server | Any RFC 5321-compliant server |
| An IMAP server | Any RFC 3501-compliant server with TLS |

---

## Installation

1. **Clone the repository**

   ```bash
   git clone https://github.com/Yukthi-Systems/WebMail-RMQ-Worker.git
   cd WebMail-RMQ-Worker
   ```

2. **Download dependencies**

   ```bash
   go mod tidy
   ```

3. **Copy and edit the environment file**

   ```bash
   cp .env.example .env
   # Edit .env with your actual values
   ```

---

## Configuration

All configuration is read from environment variables. When running locally, copy `.env.example` to `.env` — the application loads it automatically via `godotenv`.

> **Production note:** set variables directly in the host or container environment. Variables already present in the process environment always take priority over `.env`.

### RabbitMQ

| Variable | Default | Description |
|---|---|---|
| `RB_HOST` | `0.0.0.0` | RabbitMQ server hostname or IP |
| `RB_PORT` | `5672` | RabbitMQ AMQP port |
| `RB_USERNAME` | `rabbitmq_username` | AMQP authentication username |
| `RB_PASSWD` | `rabbitmq_password` | AMQP authentication password |
| `RB_VHOST` | `/` | RabbitMQ virtual host |
| `RB_PREFETCH` | `1` | Channel-level QoS prefetch count |
| `RB_MAIN_QUEUE` | `send_email` | Name of the primary work queue |
| `RB_DEAD_QUEUE` | `dead_emails` | Name of the dead-letter queue |
| `RB_CONSUME_PREFETCH` | `5` | Per-consumer prefetch count |
| `RB_WORKER_COUNT` | `1` | Number of concurrent goroutine workers |
| `RB_CONSUMER_TAG` | `emails-consumer-2` | AMQP consumer identifier |
| `AMQPR_CON_DELAY` | `5s` | Reconnect delay (Go duration string, e.g. `5s`) |

### Logging

| Variable | Default | Description |
|---|---|---|
| `LOG_FILE` | `./logs/webmail-rmq-worker.log` | Path to the rotating log file |
| `LOG_MAX_SIZE_MB` | `50` | Maximum log file size in MB before rotation |
| `LOG_MAX_BACKUPS` | `5` | Number of rotated log files to retain |
| `LOG_MAX_AGE_DAYS` | `14` | Maximum age of a log file in days |
| `LOG_COMPRESS` | `true` | Gzip-compress rotated log files |
| `LOG_LEVEL` | `debug` | Minimum log level (`trace`/`debug`/`info`/`warn`/`error`/`fatal`) |
| `LOG_CONSOLE` | `false` | Also write logs to stderr |

---

## Running the Application

### Local (Go)

```bash
go run ./cmd/worker/
```

The worker connects to RabbitMQ, declares the queues (if needed), and begins consuming messages.

### Docker

Build and run the image:

```bash
docker build -f Dokcerfile -t webmail-rmq-worker .

docker run --rm \
  --env-file .env \
  webmail-rmq-worker
```

### Docker Compose

A `docker-compose.yml` is included for local orchestration with RabbitMQ:

```bash
docker compose up --build
```

To run in the background:

```bash
docker compose up -d --build
```

---

## Queue Message Payload

Publish a JSON message to the main queue (`RB_MAIN_QUEUE`). The required header `queue_msg_id` must be set on the AMQP message for tracing.

### Minimal send example

```json
{
  "is_draft": false,
  "from":    { "name": "Alice", "email": "alice@example.com" },
  "to":      [{ "name": "Bob",  "email": "bob@example.com" }],
  "subject": "Hello from WebMail",
  "body_text": "Plain text body.",
  "body_html": "<p>HTML body.</p>",
  "folder_path": "Sent",
  "timestamp": "2026-07-15T06:00:00Z",
  "headers": {
    "Message-ID": "<unique-id@example.com>"
  },
  "server_details": {
    "smtp": {
      "server":   "smtp.example.com",
      "port":     465,
      "user":     "alice@example.com",
      "password": "secret",
      "security": "tls"
    },
    "imap": {
      "server":   "imap.example.com",
      "port":     993,
      "user":     "alice@example.com",
      "password": "secret"
    }
  }
}
```

### Draft / update-draft example

```json
{
  "is_draft":    true,
  "draft_saved": true,
  "draft_folder_name": "Drafts",
  "draft_message_id":  "<old-draft-id@example.com>",
  "folder_path": "Drafts",
  "headers": { "Message-ID": "<new-draft-id@example.com>" },
  "..."
}
```

### Full payload schema

| Field | Type | Required | Description |
|---|---|---|---|
| `is_draft` | bool | ✓ | `true` → save as draft; `false` → send |
| `from` | Address | ✓ | Sender (`name` + `email`) |
| `to` | []Address | ✓ | Primary recipients |
| `cc` | []Address | | CC recipients |
| `bcc` | []Address | | BCC recipients |
| `reply_to` | []Address | | Reply-To addresses |
| `subject` | string | ✓ | Message subject |
| `body_text` | string | | Plain-text body |
| `body_html` | string | | HTML body |
| `attachments` | []Attachment | | File attachments (base64-encoded data) |
| `in_line_attachments` | []InLineAttachment | | Inline/embedded attachments (cid: references) |
| `folder_path` | string | ✓ | IMAP folder for storing the sent/draft copy |
| `timestamp` | string | | Message date (RFC 3339); falls back to `time.Now()` |
| `headers` | map[string]string | | Extra RFC 5322 headers (e.g. `Message-ID`) |
| `server_details.smtp` | SMTPConfig | ✓ (send) | Outbound SMTP credentials |
| `server_details.imap` | IMAPConfig | ✓ | IMAP credentials |
| `draft_saved` | bool | | `true` if an existing draft should be deleted after send |
| `draft_folder_name` | string | | IMAP folder holding the existing draft |
| `draft_message_id` | string | | `Message-ID` of the draft to delete |

**Attachment fields:**

| Field | Type | Description |
|---|---|---|
| `filename` | string | Original filename |
| `content_type` | string | MIME type (auto-detected when empty) |
| `data` | string | Base64-encoded file content |

**SMTP security values:**

| Value | Port | Description |
|---|---|---|
| `tls` | 465 | Implicit TLS (auto-detected from port 465) |
| `starttls` | 587 / 25 | STARTTLS upgrade (auto-detected) |
| `plain` | any | No encryption — not recommended for production |

---

## Project Structure

```
WebMail-RMQ-Worker/
├── cmd/
│   └── worker/
│       └── main.go              # Entry point: loads config, wires up dependencies, starts consumer
├── internal/
│   ├── config/
│   │   └── config.go            # Environment variable loading and Config struct
│   ├── consumer/
│   │   └── consumer.go          # RabbitMQ consumer, worker-pool, dead-letter routing
│   ├── email/
│   │   ├── builer.go            # RFC 822 message builder (enmime)
│   │   ├── imap_store.go        # IMAP connection pool; Append, Search, Delete operations
│   │   ├── service.go           # EmailService.ProcessMail orchestration
│   │   └── smtp_sender.go       # SMTP connection pool; SendRaw
│   ├── models/
│   │   └── models.go            # EmailPayload, SMTPConfig, IMAPConfig, Address, etc.
│   └── utils/
│       └── utils.go             # GetRecipients helper
├── pkg/
│   ├── logger/
│   │   └── logger.go            # Global zerolog logger with lumberjack rotation
│   └── rabbit/
│       └── rabbit.go            # AMQP connection/channel wrapper
├── .env.example                 # Environment variable template
├── docker-compose.yml           # Local orchestration
├── Dokcerfile                   # Multi-stage Docker build
├── go.mod
├── go.sum
└── LICENSE                      # GNU GPL v3
```

---

## Third-Party Libraries

| Library | Import Path | Version | Purpose |
|---|---|---|---|
| **go-imap/v2** | `github.com/emersion/go-imap/v2` | v2.0.0-beta.8 | IMAP4rev1 client — mailbox selection, UID search, APPEND, EXPUNGE |
| **enmime** | `github.com/jhillyerd/enmime` | v1.3.0 | RFC 822 / MIME message construction (multipart, attachments, inline images) |
| **godotenv** | `github.com/joho/godotenv` | v1.5.1 | Loads `.env` files into environment variables for local development |
| **zerolog** | `github.com/rs/zerolog` | v1.35.1 | Zero-allocation, structured JSON logger |
| **amqp** | `github.com/streadway/amqp` | v1.1.0 | AMQP 0-9-1 client for RabbitMQ (publish, consume, queue declare) |
| **lumberjack** | `gopkg.in/natefinch/lumberjack.v2` | v2.2.1 | Rolling log-file writer with size, age, and compression policies |

### Indirect dependencies

| Library | Import Path | Purpose |
|---|---|---|
| go-message | `github.com/emersion/go-message` | MIME message parsing (go-imap dependency) |
| go-sasl | `github.com/emersion/go-sasl` | SASL authentication (go-imap dependency) |
| chardet | `github.com/gogs/chardet` | Character-set detection (enmime dependency) |
| html2text | `github.com/jaytaylor/html2text` | HTML → plain-text fallback (enmime dependency) |
| go-colorable | `github.com/mattn/go-colorable` | Cross-platform color output (zerolog dependency) |
| pkg/errors | `github.com/pkg/errors` | Error wrapping utilities (enmime dependency) |

---

## Contributing

Contributions are welcome! Please follow these steps:

1. **Fork** the repository and create a new branch from `main`.
2. **Commit** your changes with clear, descriptive messages.
3. **Document** any new exported types or functions with Go doc comments.
4. **Test** your changes before opening a pull request.
5. **Open a Pull Request** — describe what you changed and why.

Please make sure your code:
- Passes `go vet ./...`
- Is formatted with `gofmt` (or `goimports`)
- Does not introduce new lint warnings

---

## License

This project is licensed under the **GNU General Public License v3.0**.  
See the [LICENSE](LICENSE) file for the full text.

Copyright © 2026 Yukthi Systems Private Limited


# WebMail RMQ Worker

> High-performance RabbitMQ worker for the WebMail platform, built with Go.

## Overview

WebMail RMQ Worker processes asynchronous background jobs such as email sending and draft storing tasks. It is designed for reliability, high throughput, and easy deployment using Docker.

## Features

- 🚀 High-performance concurrent worker pool
- 🐇 RabbitMQ consumer with automatic reconnection
- ⚙️ Configurable via environment variables
- 📝 Structured logging
- 🔄 Graceful shutdown
- 📦 Lightweight Docker image
- ❤️ Health check support

## Supported Platforms

- linux/amd64

## Quick Start

```bash
docker pull ghcr.io/yukthi-systems/webmail-rmq-worker:latest

docker run -d \
  --name webmail-rmq-worker \
  --restart unless-stopped \
  --env-file .env \
  ghcr.io/yukthi-systems/webmail-rmq-worker:latest
```

## Docker Compose

```yaml
services:
  webmail-rmq-worker:
    image: ghcr.io/yukthi-systems/webmail-rmq-worker:latest
    restart: unless-stopped
    env_file:
      - .env
```

## Configuration

| Variable | Description |
|-----------|-------------|
| RABBITMQ_URL | RabbitMQ connection URI |
| DB_URL | Database connection string |
| LOG_LEVEL | Logging level |
| WORKER_COUNT | Number of worker goroutines |

## Logging

Structured logs are written to stdout and are compatible with Docker logging drivers and centralized logging solutions.

## Health & Reliability

- Automatic RabbitMQ reconnect
- Graceful shutdown
- Configurable worker pool
- Production-ready container image

## Repository

Source: https://github.com/Yukthi-Systems/WebMail-RMQ-Worker

## License

See the LICENSE file in the GitHub repository.
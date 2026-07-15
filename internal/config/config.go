/*
Copyright (C) 2026 Yukthi Systems Private Limited

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License version 3
as published by the Free Software Foundation.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
version 3 along with this program. If not, see
<https://www.gnu.org/licenses/>.
*/

package config

import (
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// Config holds all runtime configuration values for the application.
// Values are populated by [Load] from environment variables, with
// sensible defaults applied when a variable is absent.
type Config struct {
	// RabbitMQ connection settings
	RabbitHost     string // RB_HOST – broker hostname or IP (default: 0.0.0.0)
	RabbitUsername string // RB_USERNAME – AMQP username
	RabbitPassword string // RB_PASSWD  – AMQP password
	RabbitPort     int    // RB_PORT    – AMQP port (default: 5672)
	RabbitVHost    string // RB_VHOST   – virtual host (default: /)
	RabbitPrefetch int    // RB_PREFETCH – channel-level QoS prefetch count

	// Queue names
	RabbitMainQueue string // RB_MAIN_QUEUE – primary work queue (default: send_email)
	RabbitDeadQueue string // RB_DEAD_QUEUE – dead-letter queue (default: dead_emails)

	// Consumer tuning
	RabbitConsumePrefetch int           // RB_CONSUME_PREFETCH – per-consumer prefetch
	WorkerCount           int           // RB_WORKER_COUNT     – number of goroutine workers
	ConsumerTag           string        // RB_CONSUMER_TAG     – AMQP consumer identifier
	AMQPReconnectDelay    time.Duration // AMQPR_CON_DELAY     – delay between reconnect attempts

	// Logging
	LogFile        string // LOG_FILE         – path to the rotating log file
	LogMaxSizeMB   int    // LOG_MAX_SIZE_MB  – max log file size before rotation (MB)
	LogMaxBackups  int    // LOG_MAX_BACKUPS  – number of rotated log files to retain
	LogMaxAgeDays  int    // LOG_MAX_AGE_DAYS – maximum age of a log file in days
	LogCompress    bool   // LOG_COMPRESS     – gzip-compress rotated log files
	LogLevel       string // LOG_LEVEL        – minimum log level (trace/debug/info/warn/error/fatal)
	LogShowConsole bool   // LOG_CONSOLE      – also write logs to stderr
}

// Cfg is the package-level singleton populated by [Load].
// All other packages should read configuration through this variable.
var Cfg *Config

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getint(key string, def int) int {
	v := getenv(key, "")
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}

func getbool(key string, def bool) bool {
	v := getenv(key, "")
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func getdur(key string, def time.Duration) time.Duration {
	v := getenv(key, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

// Load reads environment variables (and optionally a .env file) into [Cfg].
// It uses godotenv to load a .env file from the current working directory when
// present; variables already set in the process environment take priority.
// Load must be called once at program startup before any other package
// accesses [Cfg].
func Load() {
	_ = godotenv.Load()
	Cfg = &Config{
		RabbitHost:     getenv("RB_HOST", "0.0.0.0"),
		RabbitUsername: getenv("RB_USERNAME", "rabbitmq_username"),
		RabbitPassword: getenv("RB_PASSWD", "rabbitmq_password"),
		RabbitPort:     getint("RB_PORT", 5672),
		RabbitVHost:    getenv("RB_VHOST", "/"),
		RabbitPrefetch: getint("RB_PREFETCH", 1),

		RabbitMainQueue:       getenv("RB_MAIN_QUEUE", "send_email"),
		RabbitDeadQueue:       getenv("RB_DEAD_QUEUE", "dead_emails"),
		RabbitConsumePrefetch: getint("RB_CONSUME_PREFETCH", 5),
		WorkerCount:           getint("RB_WORKER_COUNT", 1),

		ConsumerTag:        getenv("RB_CONSUMER_TAG", "emails-consumer-2"),
		AMQPReconnectDelay: getdur("AMQPR_CON_DELAY", 5*time.Second),

		LogFile:        getenv("LOG_FILE", "./logs/webmail-rmq-worker.log"),
		LogMaxSizeMB:   getint("LOG_MAX_SIZE_MB", 50),
		LogMaxBackups:  getint("LOG_MAX_BACKUPS", 5),
		LogMaxAgeDays:  getint("LOG_MAX_AGE_DAYS", 14),
		LogCompress:    getbool("LOG_COMPRESS", true),
		LogLevel:       getenv("LOG_LEVEL", "debug"),
		LogShowConsole: getbool("LOG_CONSOLE", false),
	}
}

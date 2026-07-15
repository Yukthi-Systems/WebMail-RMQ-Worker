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

// Package logger provides a process-global structured logger backed by
// [zerolog] with automatic log-file rotation via [lumberjack].
//
// Initialise the logger once at startup by calling [Init]. After that, use
// the package-level helpers (Debug, Info, Warn, Error, …) from any goroutine;
// they are safe for concurrent use.
package logger

import (
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Options configures the logger created by [Init].
type Options struct {
	File       string // Path to the log file; parent directories are created as needed.
	MaxSizeMB  int    // Maximum size of the log file in megabytes before rotation.
	MaxBackups int    // Number of old log files to retain after rotation.
	MaxAgeDays int    // Maximum number of days to retain old log files.
	Compress   bool   // Whether to gzip-compress rotated log files.
	Level      string // Minimum log level: trace, debug, info, warn, error, or fatal.
	Console    bool   // When true, logs are also written to stderr.
}

var (
	log *zerolog.Logger
	mu  sync.RWMutex
)

// Init initialises the global logger according to opts.
// It must be called once before any logging functions are used.
// Subsequent calls replace the global logger.
func Init(opts Options) {
	lj := &lumberjack.Logger{
		Filename:   opts.File,
		MaxSize:    opts.MaxSizeMB,
		MaxBackups: opts.MaxBackups,
		MaxAge:     opts.MaxAgeDays,
		Compress:   opts.Compress,
	}

	var out zerolog.LevelWriter
	if opts.Console {
		// JSON to both file and stderr
		out = zerolog.MultiLevelWriter(os.Stderr, lj)
	} else {
		// JSON only to file
		out = zerolog.MultiLevelWriter(lj)
	}

	level := zerolog.InfoLevel
	if l, err := zerolog.ParseLevel(strings.ToLower(opts.Level)); err == nil {
		level = l
	}

	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.TimestampFieldName = "time"

	newLogger := zerolog.New(out).Level(level).With().Timestamp().Logger()

	mu.Lock()
	log = &newLogger
	mu.Unlock()

	log.Info().Msg("Logger initialized")
}

// get returns a thread-safe pointer to the global logger.
func get() *zerolog.Logger {
	mu.RLock()
	defer mu.RUnlock()
	if log == nil {
		tmp := zerolog.New(os.Stdout).With().Timestamp().Logger()
		log = &tmp
	}
	return log
}

// --- Global shorthand functions ---
func Trace() *zerolog.Event { return get().Trace() }
func Debug() *zerolog.Event { return get().Debug() }
func Info() *zerolog.Event  { return get().Info() }
func Warn() *zerolog.Event  { return get().Warn() }
func Error() *zerolog.Event { return get().Error() }
func Fatal() *zerolog.Event { return get().Fatal() }
func Panic() *zerolog.Event { return get().Panic() }

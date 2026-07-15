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

package main

import (
	"fmt"
	"os"
	"os/signal"

	"syscall"
	"time"

	"github.com/Yukthi-Systems/WebMail-RMQ-Worker/internal/config"
	"github.com/Yukthi-Systems/WebMail-RMQ-Worker/internal/consumer"
	"github.com/Yukthi-Systems/WebMail-RMQ-Worker/internal/email"
	"github.com/Yukthi-Systems/WebMail-RMQ-Worker/pkg/logger"
	"github.com/Yukthi-Systems/WebMail-RMQ-Worker/pkg/rabbit"
)

func main() {
	// Load the config file
	config.Load()
	// Intiate logger
	logger.Init(logger.Options{
		File:       config.Cfg.LogFile,
		MaxSizeMB:  config.Cfg.LogMaxSizeMB,
		MaxBackups: config.Cfg.LogMaxBackups,
		MaxAgeDays: config.Cfg.LogMaxAgeDays,
		Compress:   config.Cfg.LogCompress,
		Level:      config.Cfg.LogLevel,
		Console:    config.Cfg.LogShowConsole,
	})
	// RBMQ Connection
	rabbitURL := fmt.Sprintf(
		"amqp://%s:%s@%s:%d/%s",
		config.Cfg.RabbitUsername,
		config.Cfg.RabbitPassword,
		config.Cfg.RabbitHost,
		config.Cfg.RabbitPort,
		config.Cfg.RabbitVHost,
	)
	conn, err := rabbit.NewConnection(rabbitURL, config.Cfg.RabbitPrefetch)
	if err != nil {
		// Fatal: cannot start without a broker connection.
		logger.
			Fatal().
			Err(err).
			Msg("Cannot connect to RabbitMQ.")
	}
	defer conn.Close()

	// configure and create pools
	smtp := email.NewSMTPSenderPool(10) // pool size tune
	imap := email.NewIMAPPool(5)

	svc := email.NewEmailService(smtp, imap)
	cons := consumer.NewConsumer(conn, svc)
	if err := cons.Start(); err != nil {
		// Fatal: queue declaration or consumer registration failed — nothing to process.
		logger.
			Fatal().
			Err(err).
			Msg("Failed to start the email consumer.")
	}
	logger.Info().Msg("Worker is running and waiting for email jobs on the queue.")

	// Wait for termination signal
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	logger.
		Info().
		Msg("Shutdown signal received. Finishing any in-flight jobs and closing connections...")
	// Give in-flight workers up to 1 second to finish.
	time.Sleep(1 * time.Second)
	logger.
		Info().
		Msg("Worker shut down cleanly. Goodbye.")
}

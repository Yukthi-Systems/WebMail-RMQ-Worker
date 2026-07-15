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

// Package consumer implements the RabbitMQ message consumer.
// It declares the required queues, starts a configurable pool of goroutine
// workers, and dispatches each delivery to the email service for processing.
// Failed deliveries are published to the configured dead-letter queue (DLQ)
// with an explanatory header before the original message is acknowledged.
package consumer

import (
	"context"
	"encoding/json"
	"fmt"

	"time"

	"github.com/Yukthi-Systems/WebMail-RMQ-Worker/internal/config"
	"github.com/Yukthi-Systems/WebMail-RMQ-Worker/internal/email"
	"github.com/Yukthi-Systems/WebMail-RMQ-Worker/internal/models"
	"github.com/Yukthi-Systems/WebMail-RMQ-Worker/pkg/logger"
	"github.com/Yukthi-Systems/WebMail-RMQ-Worker/pkg/rabbit"

	amqp "github.com/streadway/amqp"
)

// Consumer ties together a RabbitMQ [rabbit.Connection] and the
// [email.EmailService] into a concurrent message-processing pipeline.
// Create one with [NewConsumer] and start it with [Consumer.Start].
type Consumer struct {
	conn   *rabbit.Connection
	svc    *email.EmailService
	ctx    context.Context
	cancel context.CancelFunc
}

// NewConsumer creates a Consumer with a cancellable context derived from
// context.Background. Call [Consumer.Start] to begin consuming messages.
func NewConsumer(conn *rabbit.Connection, svc *email.EmailService) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Consumer{conn: conn, svc: svc, ctx: ctx, cancel: cancel}
}

// Start declares the main and dead-letter queues (if not already present),
// registers the consumer on the main queue, and launches [Config.WorkerCount]
// goroutines to process incoming deliveries concurrently.
// It returns an error if queue declaration or consumer registration fails.
func (c *Consumer) Start() error {
	if err := c.conn.DeclareQueues(
		config.Cfg.RabbitMainQueue,
		config.Cfg.RabbitDeadQueue,
	); err != nil {
		return fmt.Errorf("failed to declare queues %s %s: %w",
			config.Cfg.RabbitMainQueue,
			config.Cfg.RabbitDeadQueue,
			err,
		)
	}
	msgs, err := c.conn.Consume(
		config.Cfg.RabbitMainQueue,
		config.Cfg.ConsumerTag)
	if err != nil {
		return err
	}

	// Start worker pool
	for i := range config.Cfg.WorkerCount {
		go c.worker(i, msgs)
	}
	return nil
}

func (c *Consumer) worker(id int, msgs <-chan amqp.Delivery) {
	logger.Info().
		Int("worker_id", id).
		Msg("Email worker started and ready to process jobs.")
	for {
		select {
		case <-c.ctx.Done():
			logger.Info().
				Int("worker_id", id).
				Msg("Worker received a stop signal and is shutting down.")
			return
		case d, ok := <-msgs:
			if !ok {
				logger.Info().
					Int("worker_id", id).
					Msg("Message channel was closed by the broker. Worker is stopping.")
				return
			}
			// handle with timeout to avoid stuck processing
			ctx, cancel := context.WithTimeout(c.ctx, 60*time.Second)
			err := c.handleDelivery(ctx, d)
			cancel()
			if err != nil {
				// Error: the email job failed. It will be moved to the dead-letter queue for inspection.
				logger.Error().
					Int("worker_id", id).
					Str("queue_msg_id", d.Headers["queue_msg_id"].(string)).
					Err(err).
					Msg("Email job failed. Moving the message to the dead-letter queue for manual review.")

				// add custom dead-letter reason
				headers := d.Headers
				if headers == nil {
					headers = amqp.Table{}
				}
				headers["x-dead-letter-reason"] = fmt.Sprintf("email processing. %v", err)

				// republish manually to dead queue
				err = c.conn.PublishToDLQ(config.Cfg.RabbitDeadQueue, d.Body, headers)
				if err != nil {
					// Error: the failed job could not even be moved to the dead-letter queue.
					// This message may be lost. Investigate RabbitMQ connectivity.
					logger.Error().
						Int("worker_id", id).
						Str("queue_msg_id", d.Headers["queue_msg_id"].(string)).
						Err(err).
						Msg("Could not move the failed job to the dead-letter queue.")
				}
				_ = c.conn.Ack(d) // acknowledge original message
			} else {
				_ = c.conn.Ack(d) // No issue all success
			}
		}
	}
}

func (c *Consumer) handleDelivery(ctx context.Context, d amqp.Delivery) error {
	queue_msg_id := d.Headers["queue_msg_id"].(string)
	var p models.EmailPayload
	if err := json.Unmarshal(d.Body, &p); err != nil {
		// Error: the message body is not valid JSON. This is likely a publisher bug.
		logger.Error().
			Err(err).
			Str("queue_msg_id", queue_msg_id).
			Msg("Received a message with invalid JSON format. Cannot process this job.")
		return err
	}
	return c.svc.ProcessMail(ctx, &p, queue_msg_id)
}

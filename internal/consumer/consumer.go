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
// It declares the required queues, consumes with a configurable pool of
// concurrent handler goroutines (managed by go-rabbitmq, which also handles
// reconnection transparently), and dispatches each delivery to the email
// service for processing. Failed deliveries are published to the configured
// dead-letter queue (DLQ) with an explanatory header before the original
// message is acknowledged.
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

	amqp "github.com/rabbitmq/amqp091-go"
	rabbitmq "github.com/wagslane/go-rabbitmq"
)

// Consumer ties together a RabbitMQ [rabbit.Connection] and the
// [email.EmailService] into a concurrent message-processing pipeline.
// Create one with [NewConsumer], wire it up with [Consumer.Start], then
// block on [Consumer.Run] until [Consumer.Stop] is called (typically from a
// signal handler).
type Consumer struct {
	conn     *rabbit.Connection
	svc      *email.EmailService
	consumer *rabbitmq.Consumer
}

// NewConsumer creates a Consumer. Call [Consumer.Start] to declare queues
// and register the underlying RabbitMQ consumer, then [Consumer.Run] to
// begin processing messages.
func NewConsumer(conn *rabbit.Connection, svc *email.EmailService) *Consumer {
	return &Consumer{conn: conn, svc: svc}
}

// Start declares the main and dead-letter queues (if not already present)
// and registers a RabbitMQ consumer on the main queue with
// [Config.WorkerCount] concurrent handler goroutines. It returns an error if
// queue declaration or consumer registration fails.
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

	rc, err := c.conn.NewConsumer(
		config.Cfg.RabbitMainQueue,
		config.Cfg.ConsumerTag,
		config.Cfg.WorkerCount,
		config.Cfg.RabbitPrefetch,
	)
	if err != nil {
		return err
	}
	c.consumer = rc

	return nil
}

// Run begins consuming messages and blocks until [Consumer.Stop] is called
// (go-rabbitmq reconnects transparently in the background in the meantime).
// Call it after [Consumer.Start] has succeeded.
func (c *Consumer) Run() error {
	logger.Info().
		Int("worker_count", config.Cfg.WorkerCount).
		Msg("Email worker pool started and ready to process jobs.")
	return c.consumer.Run(c.handle)
}

// Stop gracefully stops the consumer, waiting for any in-flight job to
// finish before returning. Call it once, typically from a signal handler
// running concurrently with [Consumer.Run].
func (c *Consumer) Stop() {
	c.consumer.Close()
}

func (c *Consumer) handle(d rabbitmq.Delivery) rabbitmq.Action {
	queueMsgID := d.Headers["queue_msg_id"].(string)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := c.handleDelivery(ctx, d, queueMsgID); err != nil {
		// Error: the email job failed. It will be moved to the dead-letter queue for inspection.
		logger.Error().
			Str("queue_msg_id", queueMsgID).
			Err(err).
			Msg("Email job failed. Moving the message to the dead-letter queue for manual review.")

		// add custom dead-letter reason
		headers := d.Headers
		if headers == nil {
			headers = amqp.Table{}
		}
		headers["x-dead-letter-reason"] = fmt.Sprintf("email processing. %v", err)

		// republish manually to dead queue
		if err := c.conn.PublishToDLQ(config.Cfg.RabbitDeadQueue, d.Body, rabbitmq.Table(headers)); err != nil {
			// Error: the failed job could not even be moved to the dead-letter queue.
			// This message may be lost. Investigate RabbitMQ connectivity.
			logger.Error().
				Str("queue_msg_id", queueMsgID).
				Err(err).
				Msg("Could not move the failed job to the dead-letter queue.")
		}
	}

	return rabbitmq.Ack // acknowledge original message either way
}

func (c *Consumer) handleDelivery(ctx context.Context, d rabbitmq.Delivery, queueMsgID string) error {
	var p models.EmailPayload
	if err := json.Unmarshal(d.Body, &p); err != nil {
		// Error: the message body is not valid JSON. This is likely a publisher bug.
		logger.Error().
			Err(err).
			Str("queue_msg_id", queueMsgID).
			Msg("Received a message with invalid JSON format. Cannot process this job.")
		return err
	}
	return c.svc.ProcessMail(ctx, &p, queueMsgID)
}

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

// Package rabbit provides a thin wrapper around the AMQP 0-9-1 client
// ([streadway/amqp]) for connecting to RabbitMQ, declaring queues, consuming
// messages, acknowledging deliveries, and publishing to a dead-letter queue.
package rabbit

import (
	"fmt"

	"github.com/streadway/amqp"
)

// Connection wraps an AMQP connection and a single channel.
// Create one with [NewConnection]; call [Connection.Close] when done.
type Connection struct {
	URL  string
	conn *amqp.Connection
	ch   *amqp.Channel
}

// NewConnection dials the given AMQP URL, opens a channel, and sets the
// channel-level QoS prefetch count to preFetch. Both the connection and the
// channel are stored in the returned [Connection].
// Returns an error if the dial or channel open fails.
func NewConnection(amqpURL string, preFetch int) (*Connection, error) {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	_ = ch.Qos(preFetch, 0, false)
	return &Connection{URL: amqpURL, conn: conn, ch: ch}, nil
}

// DeclareQueues ensures both the dead-letter queue and the main work queue
// exist on the broker. The dead queue is declared as a plain durable queue.
// The main queue is declared passively first; if it does not yet exist it is
// created as durable with the dead-letter exchange configured to route
// rejected messages to deadQueue.
func (c *Connection) DeclareQueues(mainQueue, deadQueue string) error {

	if _, err := c.ch.QueueDeclare(
		deadQueue,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("there is an issue with the dead queue declaration: %w", err)
	}

	_, err := c.ch.QueueDeclarePassive(mainQueue, true, false, false, false, nil)
	if err == nil {
		return nil
	}

	args := amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": deadQueue,
	}

	if _, err := c.ch.QueueDeclare(
		mainQueue,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		args,
	); err != nil {
		return fmt.Errorf("there is an issue with the main queue declaration: %w", err)
	}

	return nil
}

// Consume registers a consumer on the given queue and returns a read-only
// channel of AMQP deliveries. Auto-acknowledge is disabled; the caller must
// explicitly Ack or Nack each delivery.
func (c *Connection) Consume(queue string, consumer string) (<-chan amqp.Delivery, error) {
	msgs, err := c.ch.Consume(
		queue,
		consumer,
		false, // autoAck
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,
	)
	if err != nil {
		return nil, err
	}
	return msgs, nil
}

// Ack acknowledges a single delivery, signalling that it was processed
// successfully and can be removed from the queue.
func (c *Connection) Ack(d amqp.Delivery) error {
	return d.Ack(false)
}

// Nack negatively acknowledges a delivery. When requeue is true the broker
// will re-enqueue the message; when false it is discarded or routed to the
// dead-letter queue according to the broker configuration.
func (c *Connection) Nack(d amqp.Delivery, requeue bool) error {
	return d.Nack(false, requeue)
}

// Close gracefully shuts down the channel and connection.
// It is safe to call Close more than once.
func (c *Connection) Close() {
	if c.ch != nil {
		_ = c.ch.Close()
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

// PublishToDLQ publishes a message body directly to the named dead-letter
// queue on the default exchange, attaching the provided headers. This is used
// to manually route a failed delivery to the DLQ while preserving diagnostic
// metadata such as the failure reason.
func (c *Connection) PublishToDLQ(queue string, body []byte, headers amqp.Table) error {
	return c.ch.Publish(
		"",    // default exchange
		queue, // DLQ name
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
			Headers:     headers,
		},
	)
}

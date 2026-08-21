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

// Package rabbit provides a thin wrapper around [github.com/wagslane/go-rabbitmq]
// for connecting to RabbitMQ with automatic reconnect, declaring queues,
// consuming messages, and publishing to a dead-letter queue.
package rabbit

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	rabbitmq "github.com/wagslane/go-rabbitmq"
)

// Connection wraps a reconnecting [rabbitmq.Conn], shared between the
// consumer created via [Connection.NewConsumer] and a [rabbitmq.Publisher]
// used to dead-letter failed jobs. Create one with [NewConnection]; call
// [Connection.Close] when done.
type Connection struct {
	URL       string
	conn      *rabbitmq.Conn
	publisher *rabbitmq.Publisher
}

// NewConnection dials the given AMQP URL through go-rabbitmq. The returned
// connection reconnects automatically on connection loss; consumers and
// publishers created from it recover their channels, queues, and
// subscriptions transparently. A [rabbitmq.Publisher] used by
// [Connection.PublishToDLQ] is opened on the same connection.
func NewConnection(amqpURL string) (*Connection, error) {
	conn, err := rabbitmq.NewConn(
		amqpURL,
		rabbitmq.WithConnectionOptionsLogger(rmqLogger{}),
	)
	if err != nil {
		return nil, err
	}

	publisher, err := rabbitmq.NewPublisher(
		conn,
		rabbitmq.WithPublisherOptionsLogger(rmqLogger{}),
	)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	return &Connection{URL: amqpURL, conn: conn, publisher: publisher}, nil
}

// DeclareQueues ensures both the dead-letter queue and the main work queue
// exist on the broker. Declaration happens over a short-lived, non-reconnecting
// channel: go-rabbitmq's reconnecting [rabbitmq.Conn] only declares a
// consumer's own queue (see [Connection.NewConsumer]), so the DLQ - which
// nothing consumes from directly - has nowhere else to be declared.
//
// The dead queue is declared as a plain durable queue. The main queue is
// declared passively first; if it does not yet exist it is created as
// durable with the dead-letter exchange configured to route rejected
// messages to deadQueue.
func (c *Connection) DeclareQueues(mainQueue, deadQueue string) error {
	rawConn, err := amqp.Dial(c.URL)
	if err != nil {
		return fmt.Errorf("dial for queue declaration: %w", err)
	}
	defer rawConn.Close()

	ch, err := rawConn.Channel()
	if err != nil {
		return fmt.Errorf("open channel for queue declaration: %w", err)
	}
	defer ch.Close()

	if _, err := ch.QueueDeclare(
		deadQueue,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("there is an issue with the dead queue declaration: %w", err)
	}

	// A failed passive declare closes the channel it was issued on, so this
	// probe runs on its own channel to leave ch usable afterwards.
	probeCh, err := rawConn.Channel()
	if err != nil {
		return fmt.Errorf("open channel for main queue check: %w", err)
	}
	_, err = probeCh.QueueDeclarePassive(mainQueue, true, false, false, false, nil)
	_ = probeCh.Close()
	if err == nil {
		return nil
	}

	args := amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": deadQueue,
	}

	if _, err := ch.QueueDeclare(
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

// NewConsumer creates a [rabbitmq.Consumer] on queue, using consumerTag as
// the AMQP consumer identifier, concurrency goroutines to process
// deliveries in parallel, and prefetch as the channel's QoS prefetch count.
// The queue is assumed to already exist (see [Connection.DeclareQueues]) and
// is not redeclared.
func (c *Connection) NewConsumer(queue, consumerTag string, concurrency, prefetch int) (*rabbitmq.Consumer, error) {
	return rabbitmq.NewConsumer(
		c.conn,
		queue,
		rabbitmq.WithConsumerOptionsConsumerName(consumerTag),
		rabbitmq.WithConsumerOptionsConcurrency(concurrency),
		rabbitmq.WithConsumerOptionsQOSPrefetch(prefetch),
		rabbitmq.WithConsumerOptionsQueueNoDeclare,
		rabbitmq.WithConsumerOptionsLogger(rmqLogger{}),
	)
}

// PublishToDLQ publishes a message body directly to the named dead-letter
// queue on the default exchange, attaching the provided headers. This is used
// to manually route a failed delivery to the DLQ while preserving diagnostic
// metadata such as the failure reason.
func (c *Connection) PublishToDLQ(queue string, body []byte, headers rabbitmq.Table) error {
	return c.publisher.Publish(
		body,
		[]string{queue},
		rabbitmq.WithPublishOptionsContentType("application/json"),
		rabbitmq.WithPublishOptionsHeaders(headers),
	)
}

// Close gracefully shuts down the publisher and the underlying connection.
func (c *Connection) Close() {
	if c.publisher != nil {
		c.publisher.Close()
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

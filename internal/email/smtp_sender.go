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

// Package email contains the core email-processing logic, including SMTP
// delivery, IMAP storage, and RFC 822 message construction.
package email

import (
	"crypto/tls"
	"errors"
	"net"
	"net/smtp"

	"strconv"
	"strings"
	"time"

	"github.com/Yukthi-Systems/WebMail-RMQ-Worker/internal/models"
)

type smtpConn struct {
	client *smtp.Client
	addr   string
	cfg    models.SMTPConfig
}

// SMTPSenderPool maintains a pool of authenticated SMTP client connections
// to reduce connection-establishment overhead when sending large volumes of
// messages. The pool size is fixed at construction time via [NewSMTPSenderPool].
// If the pool is empty when a send is requested, a temporary connection is
// created on-the-fly and closed after use.
type SMTPSenderPool struct {
	pool        chan *smtpConn
	size        int
	dialTimeout time.Duration
}

// NewSMTPSenderPool creates an SMTPSenderPool with the given maximum pool size.
// The dial timeout for each connection is fixed at 10 seconds.
func NewSMTPSenderPool(size int) *SMTPSenderPool {
	return &SMTPSenderPool{
		pool:        make(chan *smtpConn, size),
		size:        size,
		dialTimeout: 10 * time.Second,
	}
}

// create a new connected smtpConn (TLS or plain depending on cfg)
func (sp *SMTPSenderPool) dial(cfg models.SMTPConfig) (*smtpConn, error) {
	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	// Auto-detect security type if not explicitly set
	security := strings.ToLower(strings.TrimSpace(cfg.Security))
	if security == "" {
		switch cfg.Port {
		case 465:
			security = "tls"
		case 587, 25:
			security = "starttls"
		default:
			security = "plain"
		}
	}
	switch security {
	case "tls":
		tlsCfg := &tls.Config{ServerName: cfg.Host, InsecureSkipVerify: false}
		conn, err := tls.DialWithDialer(
			&net.Dialer{Timeout: sp.dialTimeout},
			"tcp",
			addr,
			tlsCfg,
		)
		if err != nil {
			return nil, err
		}
		c, err := smtp.NewClient(conn, cfg.Host)
		if err != nil {
			return nil, err
		}
		auth := smtp.PlainAuth("", cfg.Username, cfg.Password, cfg.Host)
		if err = c.Auth(auth); err != nil {
			c.Close()
			return nil, err
		}
		return &smtpConn{client: c, addr: addr, cfg: cfg}, nil
	case "starttls":
		c, err := smtp.Dial(addr)
		if err != nil {
			return nil, err
		}
		if ok, _ := c.Extension("STARTTLS"); ok {
			tlsCfg := &tls.Config{ServerName: cfg.Host, InsecureSkipVerify: false}
			if err = c.StartTLS(tlsCfg); err != nil {
				c.Close()
				return nil, err
			}
		}
		auth := smtp.PlainAuth("", cfg.Username, cfg.Password, cfg.Host)
		if err = c.Auth(auth); err != nil {
			c.Close()
			return nil, err
		}
		return &smtpConn{client: c, addr: addr, cfg: cfg}, nil
	default:
		return nil, errors.New("plain send no pooling; use tls/starttls for pooling")
	}
}

// SendRaw uses pool to send the message bytes to recipients.
// If pool empty, create a temporary connection to keep latency low.
func (sp *SMTPSenderPool) SendRaw(
	cfg models.SMTPConfig,
	raw []byte,
	recipients []string,
	from string,
) error {

	// TODO : based on port change
	if cfg.Security == "plain" {
		// fallback to simple sendMail (no pooling)
		addr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
		auth := smtp.PlainAuth("", cfg.Username, cfg.Password, cfg.Host)
		return smtp.SendMail(addr, auth, from, recipients, raw)
	}

	// try to get pooled conn
	select {
	case sc := <-sp.pool:
		// use pooled client
		defer func() { sp.put(sc) }()
		return sp.sendWithClient(sc, from, recipients, raw)
	default:
		// no pooled conn available — create a temporary one
		sc, err := sp.dial(cfg)
		if err != nil {
			return err
		}
		defer sc.client.Close()
		return sp.sendWithClient(sc, from, recipients, raw)
	}
}

func (sp *SMTPSenderPool) sendWithClient(
	sc *smtpConn,
	from string,
	recipients []string,
	raw []byte,
) error {
	c := sc.client
	// Reset any previous state
	_ = c.Reset()

	if err := c.Mail(from); err != nil {
		return err
	}
	for _, rcpt := range recipients {
		if err := c.Rcpt(rcpt); err != nil {
			return err
		}
	}
	w, err := c.Data()
	if err != nil {
		return err
	}
	if _, err := w.Write(raw); err != nil {
		_ = w.Close()
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return nil
}

func (sp *SMTPSenderPool) put(sc *smtpConn) {
	// if pool is full, close connection; else return it
	select {
	case sp.pool <- sc:
	default:
		sc.client.Close()
	}
}

// WarmUp pre-dials up to [SMTPSenderPool.size] connections for the given
// configuration and adds them to the pool so that the first burst of sends
// does not incur connection-establishment latency. Stops early on the first
// dial error.
func (sp *SMTPSenderPool) WarmUp(cfg models.SMTPConfig) {
	// try to fill pool up to capacity
	for range sp.size {
		sc, err := sp.dial(cfg)
		if err != nil {
			// stop on error
			return
		}
		select {
		case sp.pool <- sc:
		default:
			sc.client.Close()
		}
	}
}

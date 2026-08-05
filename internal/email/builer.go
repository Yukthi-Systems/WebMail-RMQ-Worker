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

package email

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/mail"

	"strings"
	"time"

	"github.com/Yukthi-Systems/WebMail-RMQ-Worker/internal/models"
	"github.com/Yukthi-Systems/WebMail-RMQ-Worker/pkg/logger"
	"github.com/jhillyerd/enmime"
)

// BuildEmail constructs a fully encoded RFC 822 email message from the given
// [models.EmailPayload] and returns the raw bytes ready to be passed directly
// to an SMTP DATA command or appended to an IMAP mailbox.
//
// The function handles:
//   - Plain-text and HTML bodies (multipart/alternative when both are present)
//   - Custom RFC 5322 headers (e.g. Message-ID, In-Reply-To)
//   - Base64-encoded file attachments (auto-detects MIME type when omitted)
//   - Inline (embedded) attachments referenced via cid: URIs
//
// If p.Timestamp is not a valid RFC 3339 string the current UTC time is used
// as the message Date header and a warning is logged.
func BuildEmail(p *models.EmailPayload) ([]byte, error) {
	parsedTime, err := time.Parse(time.RFC3339, p.Timestamp)
	if err != nil {
		// Warn: the timestamp field is missing or incorrectly formatted.
		// The message will still be built using the current time as the Date header.
		logger.Warn().
			Str("from", p.From.Address).
			Str("subject", p.Subject).
			Str("received_timestamp", p.Timestamp).
			Err(err).
			Msg("The email timestamp is missing or not in RFC 3339 format.")
		parsedTime = time.Now().UTC()
	}
	b := enmime.Builder().
		From(p.From.Name, p.From.Address).
		Subject(p.Subject).
		Date(parsedTime)

	for key, value := range p.Headers {
		b = b.Header(key, value)
	}

	// Request a read receipt (Message Disposition Notification) the same way
	// Roundcube does: add a Disposition-Notification-To header pointing back
	// at the sender. Recipients' mail clients decide whether to honor it.
	if p.ReadReceipt {
		b = b.Header("Disposition-Notification-To", (&mail.Address{Name: p.From.Name, Address: p.From.Address}).String())
	}

	// Add recipients
	for _, addr := range p.To {
		b = b.To(addr.Name, addr.Address)
	}
	for _, addr := range p.Cc {
		b = b.CC(addr.Name, addr.Address)
	}
	for _, addr := range p.Bcc {
		b = b.BCC(addr.Name, addr.Address)
	}
	for _, addr := range p.ReplyTo {
		b = b.ReplyTo(addr.Name, addr.Address)
	}

	// Add message body
	if p.TextBody != "" {
		b = b.Text([]byte(p.TextBody))
	}
	if p.HTMLBody != "" {
		b = b.HTML([]byte(p.HTMLBody))
	}

	// TODO : inline data future.
	if len(p.InLineAttachments) > 0 {
		for _, inlineAtt := range p.InLineAttachments {
			if inlineAtt.Data == "" {
				// Warn: the inline attachment was declared but has no content. It will be skipped.
				logger.Warn().
					Str("from", p.From.Address).
					Str("subject", p.Subject).
					Str("filename", inlineAtt.Filename).
					Msg("An inline attachment has no data and will be skipped.")
				continue
			}
			inlineFileDataInBytes, err := base64.StdEncoding.DecodeString(inlineAtt.Data)
			if err != nil {
				// Error: the inline attachment data is not valid base64. The attachment will be skipped.
				logger.Error().
					Str("from", p.From.Address).
					Str("subject", p.Subject).
					Str("filename", inlineAtt.Filename).
					Err(err).
					Msg("The inline attachment data could not be decoded (invalid base64).")
				continue
			}
			mime := inlineAtt.MimeType
			if mime == "" {
				mime = http.DetectContentType(inlineFileDataInBytes)
			}
			// If it's a text type and lacks charset, explicitly add UTF-8
			if (strings.HasPrefix(mime, "text/") || strings.Contains(mime, "xml")) &&
				!strings.Contains(strings.ToLower(mime), "charset") {
				mime += "; charset=utf-8"
			}
			// Debug: logged per attachment so developers can verify size and content type.
			logger.Debug().
				Str("from", p.From.Address).
				Str("subject", p.Subject).
				Str("content_id", inlineAtt.Filename).
				Str("mime_type", mime).
				Int("size_bytes", len(inlineFileDataInBytes)).
				Msg("Inline attachment added to the message.")
			b = b.AddInline(inlineFileDataInBytes, mime, inlineAtt.Filename, inlineAtt.Filename)
		}
	}

	// Add attachments
	for _, att := range p.Attachments {
		if att.Data == "" {
			// Warn: the attachment was declared but has no content. It will be skipped.
			logger.Warn().
				Str("from", p.From.Address).
				Str("subject", p.Subject).
				Str("filename", att.Filename).
				Msg("An attachment has no data and will be skipped.")
			continue
		}
		fileDataInBytes, err := base64.StdEncoding.DecodeString(att.Data)
		if err != nil {
			// Error: the attachment data is not valid base64. The attachment will be skipped.
			logger.Error().
				Str("from", p.From.Address).
				Str("subject", p.Subject).
				Str("filename", att.Filename).
				Err(err).
				Msg("The attachment data could not be decoded (invalid base64).")
			continue
		}
		mime := att.MimeType
		if mime == "" {
			mime = http.DetectContentType(fileDataInBytes)
		}
		// If it's a text type and lacks charset, explicitly add UTF-8
		if (strings.HasPrefix(mime, "text/") || strings.Contains(mime, "xml")) &&
			!strings.Contains(strings.ToLower(mime), "charset") {
			mime += "; charset=utf-8"
		}

		// Debug: logged per attachment so developers can verify size and content type.
		logger.Debug().
			Str("from", p.From.Address).
			Str("subject", p.Subject).
			Str("filename", att.Filename).
			Str("mime_type", mime).
			Int("size_bytes", len(fileDataInBytes)).
			Msg("Attachment added to the message.")
		b = b.AddAttachment(fileDataInBytes, mime, att.Filename)
	}

	root, err := b.Build()
	if err != nil {
		return nil, fmt.Errorf("RFC build: %w", err)
	}

	var buf bytes.Buffer
	if err := root.Encode(&buf); err != nil {
		return nil, fmt.Errorf("RFC build encode error: %w", err)
	}
	return buf.Bytes(), nil
}

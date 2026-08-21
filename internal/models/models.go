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

// Package models defines the data structures used to represent email payloads
// and mail-server connection configuration exchanged through the RabbitMQ queue.
package models

// Attachment represents a file that is attached to an outbound email message.
// The file content must be base64-encoded and stored in Data.
type Attachment struct {
	Filename string `json:"filename"`               // Original filename shown to recipients.
	MimeType string `json:"content_type,omitempty"` // MIME type, e.g. "application/pdf". Auto-detected when empty.
	Data     string `json:"data"`                   // Base64-encoded file content.
}

// InLineAttachment represents an embedded (inline) image or resource within
// the HTML body of an email. The Filename field is also used as the Content-ID
// so that it can be referenced via cid: URIs inside the HTML body.
type InLineAttachment struct {
	MimeType string `json:"content_type,omitempty"` // MIME type, e.g. "image/png". Auto-detected when empty.
	Data     string `json:"data"`                   // Base64-encoded file content.
	Filename string `json:"filename"`               // Filename and Content-ID used in cid: references.
}

// SMTPConfig holds the credentials and transport settings for an outbound
// SMTP connection. Security controls which TLS handshake strategy is used:
//
//   - "tls"      – implicit TLS (port 465)
//   - "starttls" – STARTTLS upgrade (port 587 / 25)
//   - "plain"    – no encryption (not recommended for production)
//
// When Security is empty the correct mode is inferred from Port.
type SMTPConfig struct {
	Host     string `json:"server"`             // SMTP server hostname or IP.
	Port     int    `json:"port"`               // SMTP server port.
	Username string `json:"user"`               // Authentication username (usually the sender address).
	Password string `json:"password"`           // Authentication password.
	Security string `json:"security,omitempty"` // Transport security: "tls", "starttls", or "plain".
}

// IMAPConfig holds the credentials and connection settings for an IMAP server.
// Only TLS connections are currently supported.
type IMAPConfig struct {
	Host     string `json:"server"`        // IMAP server hostname or IP.
	Port     int    `json:"port"`          // IMAP server port (usually 993 for TLS).
	Username string `json:"user"`          // Authentication username (usually the mailbox address).
	Password string `json:"password"`      // Authentication password.
	TLS      bool   `json:"tls,omitempty"` // Reserved for future use; TLS is always enabled.
}

// Address represents an RFC 5321 email address with an optional display name.
type Address struct {
	Name    string `json:"name"`  // Display name; may be empty.
	Address string `json:"email"` // Email address in user@domain form.
}

// ServerDetails bundles the outbound SMTP and inbound IMAP credentials
// that are required to send a message and store a copy in the Sent folder.
type ServerDetails struct {
	SMTP SMTPConfig `json:"smtp,omitempty"` // Outbound SMTP configuration.
	IMAP IMAPConfig `json:"imap"`           // Inbound IMAP configuration used to store sent/draft mail.
}

// EmailPayload is the JSON structure published to the RabbitMQ work queue.
// Consumers unmarshal each delivery into an EmailPayload and pass it to
// [email.EmailService.ProcessMail] for further handling.
//
// Draft flow:
//   - Set IsDraft to true to store (or update) the message in the IMAP
//     folder specified by FolderPath without sending it via SMTP.
//   - If the draft was previously saved (DraftSaved == true), the worker
//     will delete the old copy identified by DraftMessageID from
//     DraftFolderName after the new message has been appended.
//
// Send flow:
//   - Set IsDraft to false. The message is delivered via SMTP and a copy is
//     appended to FolderPath (typically the Sent folder) over IMAP.
type EmailPayload struct {
	IsDraft           bool               `json:"is_draft"`                      // True → save as draft; false → send via SMTP.
	From              Address            `json:"from"`                          // Sender address.
	To                []Address          `json:"to"`                            // Primary recipients.
	Cc                []Address          `json:"cc,omitempty"`                  // Carbon-copy recipients.
	Bcc               []Address          `json:"bcc,omitempty"`                 // Blind carbon-copy recipients.
	Subject           string             `json:"subject"`                       // Message subject line.
	ReplyTo           []Address          `json:"reply_to,omitempty"`            // Reply-To addresses.
	TextBody          string             `json:"body_text,omitempty"`           // Plain-text body.
	HTMLBody          string             `json:"body_html,omitempty"`           // HTML body.
	Attachments       []Attachment       `json:"attachments,omitempty"`         // Regular file attachments.
	InLineAttachments []InLineAttachment `json:"in_line_attachments,omitempty"` // Inline (embedded) attachments.
	FolderPath        string             `json:"folder_path"`                   // IMAP folder to append the message to.
	ServerDetails     ServerDetails      `json:"server_details"`                // SMTP and IMAP connection details.
	Headers           map[string]string  `json:"headers"`                       // Additional RFC 5322 headers, e.g. Message-ID.
	Timestamp         string             `json:"timestamp"`                     // Message date in RFC 3339 format.
	DraftSaved        bool               `json:"draft_saved"`                   // True if a previous draft exists and should be removed after sending.
	DraftFolderName   string             `json:"draft_folder_name,omitempty"`   // IMAP folder containing the existing draft.
	DraftMessageID    string             `json:"draft_message_id,omitempty"`    // Message-ID of the existing draft to delete.
	ReadReceipt       bool               `json:"read_receipt,omitempty"`        // True to request a read receipt from recipients.
}

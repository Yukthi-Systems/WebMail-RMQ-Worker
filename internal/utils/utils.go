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

// Package utils provides general-purpose helper functions shared across
// internal packages.
package utils

import (
	"errors"

	"github.com/Yukthi-Systems/WebMail-RMQ-Worker/internal/models"
)

// GetRecipients collects all recipient email addresses from the To, Cc, and
// Bcc fields of the payload into a flat string slice suitable for passing to
// an SMTP client. It returns an error if no recipients are found.
func GetRecipients(p *models.EmailPayload) ([]string, error) {
	totalRecipients := len(p.To) + len(p.Cc) + len(p.Bcc)
	recipients := make([]string, 0, totalRecipients)

	extractAddresses := func(addresses []models.Address) {
		for _, addr := range addresses {
			recipients = append(recipients, addr.Address)
		}
	}
	extractAddresses(p.To)
	extractAddresses(p.Cc)
	extractAddresses(p.Bcc)

	if len(recipients) == 0 {
		return nil, errors.New("there is no recipient to send the email to.")
	}
	return recipients, nil
}

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
	"context"
	"fmt"

	"github.com/Yukthi-Systems/WebMail-RMQ-Worker/internal/models"
	"github.com/Yukthi-Systems/WebMail-RMQ-Worker/internal/utils"
	"github.com/Yukthi-Systems/WebMail-RMQ-Worker/pkg/logger"
)

// EmailService orchestrates the end-to-end processing of an [models.EmailPayload].
// It composes the raw RFC 822 message, then either stores it as an IMAP draft
// or delivers it via SMTP and saves a copy to the Sent folder.
type EmailService struct {
	smtpPool *SMTPSenderPool
	imapPool *IMAPPool
}

// NewEmailService creates an EmailService that delegates SMTP delivery to
// smtpPool and IMAP operations to imapPool.
func NewEmailService(smtpPool *SMTPSenderPool, imapPool *IMAPPool) *EmailService {
	return &EmailService{smtpPool: smtpPool, imapPool: imapPool}
}

// ProcessMail processes a single email payload identified by queue_msg_id.
//
// Draft mode (p.IsDraft == true):
//  1. Search the IMAP folder for an existing message with the same Message-ID.
//  2. Delete the old draft if found.
//  3. Append the newly built message to the IMAP folder.
//
// Send mode (p.IsDraft == false):
//  1. Send the message via SMTP.
//  2. Append a copy to the IMAP Sent folder.
//  3. If a saved draft exists (p.DraftSaved == true), locate and delete it.
//
// ctx should carry a deadline; the caller (consumer worker) applies a 60-second
// timeout per message.
func (s *EmailService) ProcessMail(
	ctx context.Context,
	p *models.EmailPayload,
	queue_msg_id string,
) error {
	// Info: one line per job so operators can track every email through the system.
	logger.Info().
		Str("queue_msg_id", queue_msg_id).
		Str("from", p.From.Address).
		Interface("to", p.To).
		Interface("cc", p.Cc).
		Interface("bcc", p.Bcc).
		Str("subject", p.Subject).
		Bool("is_draft", p.IsDraft).
		Str("message_id", p.Headers["Message-ID"]).
		Bool("draft_saved", p.DraftSaved).
		Str("draft_folder", p.DraftFolderName).
		Str("draft_message_id", p.DraftMessageID).
		Bool("read_receipt", p.ReadReceipt).
		Msg("New email job received. Starting to process.")

	raw, err := BuildEmail(p)
	if err != nil {
		// Error: the email message could not be composed.
		// Check the payload fields (headers, body, attachments).
		logger.Error().
			Err(err).
			Str("queue_msg_id", queue_msg_id).
			Msg("Failed to compose the email message.")
		return fmt.Errorf("BuildEmail %w", err)
	}
	// Debug: raw byte size is only useful for developers diagnosing build issues.
	logger.Debug().
		Str("queue_msg_id", queue_msg_id).
		Int("raw_size_bytes", len(raw)).
		Msg("Email message composed successfully. Proceeding to delivery.")

	if p.IsDraft {
		// Debug: searching for an existing draft with the same Message-ID to replace it.
		logger.Debug().
			Str("queue_msg_id", queue_msg_id).
			Str("folder", p.FolderPath).
			Str("message_id", p.Headers["Message-ID"]).
			Msg("Draft mode: checking whether an older version of this draft already exists in the mailbox.")

		uids, err := s.imapPool.SearchByMessageID(
			ctx,
			p.ServerDetails.IMAP,
			p.FolderPath,
			p.Headers["Message-ID"],
		)
		if err != nil {
			// Error: could not reach the IMAP server to look for the draft.
			logger.Error().
				Err(err).
				Str("queue_msg_id", queue_msg_id).
				Str("imap_host", p.ServerDetails.IMAP.Host).
				Msg("Could not search the mailbox for an existing draft.")
			return err
		}
		logger.Debug().
			Str("queue_msg_id", queue_msg_id).
			Int("existing_drafts_found", len(uids)).
			Msg("Mailbox search complete.")

		if len(uids) != 0 {
			// Debug: removing the previous draft before saving the updated one.
			logger.Debug().
				Str("queue_msg_id", queue_msg_id).
				Interface("uids", uids).
				Msg("An older version of this draft was found. Removing it before saving the updated draft.")
			if err := s.imapPool.DeleteByUIDs(ctx, p.ServerDetails.IMAP, p.FolderPath, uids); err != nil {
				// Error: the old draft could not be deleted; abort to avoid creating duplicate drafts.
				logger.Error().
					Err(err).
					Str("queue_msg_id", queue_msg_id).
					Msg("Failed to remove the older draft from the mailbox. Aborting to prevent duplicate drafts.")
				return err
			}
			logger.Debug().
				Str("queue_msg_id", queue_msg_id).
				Msg("Older draft removed from the mailbox.")
		}

		// Debug: the new draft content is about to be stored in the IMAP folder.
		logger.Debug().
			Str("queue_msg_id", queue_msg_id).
			Str("folder", p.FolderPath).
			Msg("Saving the new draft to the mailbox.")

		if err := s.imapPool.Append(ctx, p.ServerDetails.IMAP, raw, p.FolderPath); err != nil {
			// Error: the new draft could not be stored on the IMAP server.
			logger.Error().
				Err(err).
				Str("queue_msg_id", queue_msg_id).
				Str("folder", p.FolderPath).
				Msg("Failed to save the draft to the mailbox. Check IMAP server connectivity.")
			return err
		}

		// Info: draft save is the final successful outcome for a draft job.
		logger.Info().
			Str("queue_msg_id", queue_msg_id).
			Str("folder", p.FolderPath).
			Msg("Draft saved to the mailbox successfully. Job complete.")

		return nil
	}

	// Sending Email (non-draft)
	// Debug: listing who the email will be delivered to.
	logger.Debug().
		Str("queue_msg_id", queue_msg_id).
		Msg("Send mode: collecting all recipient addresses (To, Cc, Bcc).")

	recipients, err := utils.GetRecipients(p)
	if err != nil {
		// Error: the payload has no recipients at all — nothing to send.
		logger.Error().
			Err(err).
			Str("queue_msg_id", queue_msg_id).
			Interface("to", p.To).
			Interface("cc", p.Cc).
			Interface("bcc", p.Bcc).
			Msg("The email has no recipients (To, Cc, and Bcc are all empty).")
		return err
	}

	logger.Debug().
		Str("queue_msg_id", queue_msg_id).
		Strs("recipients", recipients).
		Int("recipient_count", len(recipients)).
		Msg("Recipients collected. Connecting to the SMTP server to send the email.")

	sendErr := s.smtpPool.SendRaw(p.ServerDetails.SMTP, raw, recipients, p.From.Address)
	if sendErr != nil {
		// Error: SMTP delivery failed. This could be wrong credentials,
		// a network issue, or a server rejection.
		logger.Error().
			Err(sendErr).
			Str("queue_msg_id", queue_msg_id).
			Str("smtp_host", p.ServerDetails.SMTP.Host).
			Int("smtp_port", p.ServerDetails.SMTP.Port).
			Strs("recipients", recipients).
			Msg("Failed to deliver the email via SMTP.")
		return sendErr
	}

	// Info: the email has left the server — this is the key success milestone for operators.
	logger.Info().
		Str("queue_msg_id", queue_msg_id).
		Str("from", p.From.Address).
		Strs("recipients", recipients).
		Msg("Email delivered successfully via SMTP.")

	// Debug: store a copy in the IMAP Sent folder so the sender can see it in their Sent box.
	logger.Debug().
		Str("queue_msg_id", queue_msg_id).
		Str("folder", p.FolderPath).
		Msg("Copying the sent message to the Sent folder on the IMAP server.")

	if err := s.imapPool.Append(ctx, p.ServerDetails.IMAP, raw, p.FolderPath); err != nil {
		// Error: email was sent but the copy in Sent folder failed.
		// Log and return so the job can be retried.
		logger.Error().
			Err(err).
			Str("queue_msg_id", queue_msg_id).
			Str("folder", p.FolderPath).
			Str("imap_host", p.ServerDetails.IMAP.Host).
			Msg("Email was sent but could not be saved to the Sent folder.")
		return err
	}
	if p.DraftSaved {
		uids, err := s.imapPool.SearchByMessageID(
			ctx,
			p.ServerDetails.IMAP,
			p.DraftFolderName,
			p.DraftMessageID,
		)
		if err != nil {
			// Warn: draft cleanup failed but the email was already delivered, so do not abort.
			logger.Warn().
				Err(err).
				Str("queue_msg_id", queue_msg_id).
				Str("imap_host", p.ServerDetails.IMAP.Host).
				Msg("Email was sent, but searching for the original draft to clean up failed.")
		}
		if len(uids) != 0 {
			if err := s.imapPool.DeleteByUIDs(
				ctx,
				p.ServerDetails.IMAP,
				p.DraftFolderName,
				uids,
			); err != nil {
				// Error: the draft still exists even though the email was sent.
				// This is not critical but should be investigated.
				logger.Error().
					Err(err).
					Str("queue_msg_id", queue_msg_id).
					Msg("Could not remove the original draft after sending.")
				return err
			}
			logger.Debug().
				Str("queue_msg_id", queue_msg_id).
				Interface("uids", uids).
				Msg("Original draft removed from the Drafts folder after sending.")
		} else {
			// Warn: the draft was expected but not found.
			// Not a hard failure, but worth noting.
			logger.Warn().
				Str("queue_msg_id", queue_msg_id).
				Str("draft_folder", p.DraftFolderName).
				Str("draft_message_id", p.DraftMessageID).
				Msg("Could not find the original draft after sending.")
		}
	}

	// Info: all steps completed — job is done.
	logger.Info().
		Str("queue_msg_id", queue_msg_id).
		Msg("Email job finished. Message sent and stored in the Sent folder.")

	return nil
}

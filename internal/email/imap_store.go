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
	"crypto/tls"
	"fmt"
	"net"

	"strings"
	"time"

	"github.com/Yukthi-Systems/WebMail-RMQ-Worker/internal/models"
	"github.com/Yukthi-Systems/WebMail-RMQ-Worker/pkg/logger"
	"github.com/emersion/go-imap/v2"
	"github.com/emersion/go-imap/v2/imapclient"
)

type imapConn struct {
	client *imapclient.Client
	cfg    models.IMAPConfig
}

// IMAPPool maintains a pool of authenticated IMAP client connections.
// Connections are reused across Append operations to avoid repeated TLS
// handshakes. SearchByMessageID and DeleteByUIDs always open dedicated
// connections because they require exclusive mailbox selection.
type IMAPPool struct {
	pool chan *imapConn
	size int
}

// NewIMAPPool creates an IMAPPool with the specified maximum pool capacity.
func NewIMAPPool(size int) *IMAPPool {
	return &IMAPPool{
		pool: make(chan *imapConn, size),
		size: size,
	}
}

// dial creates a new IMAP client using TLS
func (mp *IMAPPool) dial(
	ctx context.Context,
	cfg models.IMAPConfig) (*imapConn, error) {
	addr := net.JoinHostPort(cfg.Host, fmt.Sprintf("%d", cfg.Port))

	errCh := make(chan error, 1)
	var (
		client *imapclient.Client
		conn   net.Conn
	)

	go func() {
		dialer := &net.Dialer{}
		tlsCfg := &tls.Config{ServerName: cfg.Host}

		c, err := tls.DialWithDialer(dialer, "tcp", addr, tlsCfg)
		if err != nil {
			errCh <- err
			return
		}
		conn = c
		client = imapclient.New(c, &imapclient.Options{})

		err = client.Login(cfg.Username, cfg.Password).Wait()
		errCh <- err
	}()

	select {
	case <-ctx.Done():
		if conn != nil {
			conn.Close()
		}
		return nil, fmt.Errorf("dial canceled: %w", ctx.Err())
	case err := <-errCh:
		if err != nil {
			if conn != nil {
				conn.Close()
			}
			return nil, fmt.Errorf("login: %w", err)
		}
	}

	return &imapConn{client: client, cfg: cfg}, nil
}

// Append appends a raw email message to the given mailbox (e.g., INBOX, Sent)
func (mp *IMAPPool) Append(
	ctx context.Context,
	cfg models.IMAPConfig,
	raw []byte,
	folderPath string) error {
	select {
	case ic := <-mp.pool:

		err := mp.appendWithClient(ctx, ic, raw, folderPath)
		mp.put(ic, err)
		return err
	default:
		ic, err := mp.dial(ctx, cfg)
		if err != nil {
			return err
		}
		defer func() {
			_ = ic.client.Logout().Wait()
			ic.client.Close()
		}()
		return mp.appendWithClient(ctx, ic, raw, folderPath)
	}
}

func (mp *IMAPPool) appendWithClient(
	ctx context.Context,
	ic *imapConn,
	raw []byte,
	folderPath string) error {

	opts := &imap.AppendOptions{
		Flags: []imap.Flag{"\\Seen"},
		Time:  time.Now(),
	}
	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("panic: %v", r)
			}
		}()

		appendCmd := ic.client.Append(folderPath, int64(len(raw)), opts)

		_, err := appendCmd.Write(raw)
		if err != nil {
			_ = appendCmd.Close()
			_, _ = appendCmd.Wait()

			done <- fmt.Errorf("append write: %w", err)
			return
		}
		if err := appendCmd.Close(); err != nil {
			done <- fmt.Errorf("append close: %w", err)
			return
		}
		if _, err := appendCmd.Wait(); err != nil {
			done <- fmt.Errorf("append wait: %w", err)
			return
		}

		done <- nil
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("append canceled or timed out: %w", ctx.Err())
	case err := <-done:
		return err
	}
}

func (mp *IMAPPool) put(ic *imapConn, err error) {
	if err != nil {
		msg := err.Error()

		if strings.Contains(msg, "OVERQUOTA") ||
			strings.Contains(msg, "TRYCREATE") ||
			strings.Contains(msg, "NO ") {
			select {
			case mp.pool <- ic:
			default:
				_ = ic.client.Logout().Wait()
				ic.client.Close()
			}
			return
		}
		_ = ic.client.Logout().Wait()
		ic.client.Close()
		return
	}

	select {
	case mp.pool <- ic:
	default:
		_ = ic.client.Logout().Wait()
		ic.client.Close()
	}
}

// WarmUp pre-dials up to [IMAPPool.size] connections and places them in the
// pool so that early Append calls do not pay the connection cost. Stops on
// the first dial error.
func (mp *IMAPPool) WarmUp(ctx context.Context, cfg models.IMAPConfig) {
	for i := 0; i < mp.size; i++ {
		ic, err := mp.dial(ctx, cfg)
		if err != nil {
			logger.Error().
				Err(err).
				Msg("warmup failed")
			return
		}
		select {
		case mp.pool <- ic:
		default:
			_ = ic.client.Logout().Wait()
			ic.client.Close()
		}
	}
}

// SearchByMessageID returns the UIDs of messages matching a given Message-ID
// in a folder.
func (mp *IMAPPool) SearchByMessageID(
	ctx context.Context,
	cfg models.IMAPConfig,
	folder,
	messageID string,
) ([]imap.UID, error) {
	ic, err := mp.dial(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer func() {
		_ = ic.client.Logout().Wait()
		ic.client.Close()
	}()
	if _, err := ic.client.Select(folder, nil).Wait(); err != nil {
		return nil, fmt.Errorf("select folder %s: %w", folder, err)
	}

	criteria := &imap.SearchCriteria{
		Header: []imap.SearchCriteriaHeaderField{{
			Key:   "Message-ID",
			Value: messageID,
		}},
	}

	searchCmd := ic.client.UIDSearch(criteria, nil)
	waitCh := make(chan struct {
		uids []imap.UID
		err  error
	}, 1)

	go func() {
		d, err := searchCmd.Wait()
		if err != nil {
			waitCh <- struct {
				uids []imap.UID
				err  error
			}{nil, err}
			return
		}
		uids := d.AllUIDs()
		waitCh <- struct {
			uids []imap.UID
			err  error
		}{uids, nil}
	}()

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("search timeout: %w", ctx.Err())
	case res := <-waitCh:
		if res.err != nil {
			return nil, fmt.Errorf("search: %w", res.err)
		}
		if len(res.uids) == 0 {
			return nil, nil
		}
		return res.uids, nil
	}
}

// DeleteByUIDs marks messages as \Deleted and expunges them
func (mp *IMAPPool) DeleteByUIDs(
	ctx context.Context,
	cfg models.IMAPConfig,
	folder string,
	uids []imap.UID,
) error {
	targetUID := uids[0]

	ic, err := mp.dial(ctx, cfg)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer func() {
		_ = ic.client.Logout().Wait()
		ic.client.Close()
	}()

	if _, err := ic.client.Select(folder, nil).Wait(); err != nil {
		return fmt.Errorf("select folder: %w", err)
	}

	uidSet := imap.UIDSet{}
	uidSet.AddNum(targetUID)

	flags := imap.StoreFlags{
		Op:    imap.StoreFlagsAdd,
		Flags: []imap.Flag{imap.FlagDeleted},
	}

	storeCmd := ic.client.Store(uidSet, &flags, nil)
	storeCh := make(chan error, 1)
	go func() {
		storeCh <- storeCmd.Close()
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("store canceled: %w", ctx.Err())
	case err := <-storeCh:
		if err != nil {
			return fmt.Errorf("store: %w", err)
		}
	}

	expungeCmd := ic.client.Expunge()
	expungeCh := make(chan struct {
		seq []uint32
		err error
	}, 1)

	go func() {
		seq, err := expungeCmd.Collect()
		expungeCh <- struct {
			seq []uint32
			err error
		}{seq, err}
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("expunge canceled: %w", ctx.Err())
	case res := <-expungeCh:
		if res.err != nil {
			return fmt.Errorf("expunge: %w", res.err)
		}
		logger.Info().
			Uint32("uid", uint32(targetUID)).
			Int("messages_expunged", len(res.seq)).
			Str("folder", folder).
			Msg("Message permanently deleted from the mailbox.")
	}

	return nil
}

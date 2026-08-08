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

package rabbit

import (
	"fmt"

	"github.com/Yukthi-Systems/WebMail-RMQ-Worker/pkg/logger"
)

// rmqLogger adapts the process-global zerolog-backed [logger] package to the
// Fatalf/Errorf/Warnf/Infof/Debugf interface go-rabbitmq expects, so
// connection/consumer/publisher lifecycle events (reconnects, channel
// errors, ...) land in the same log file as everything else.
//
// Fatalf is mapped to Error rather than Fatal: go-rabbitmq never actually
// calls it internally (it's only reachable through custom code), but a
// worker process reconnecting to a broker should never be able to trigger
// an os.Exit via a logging call.
type rmqLogger struct{}

func (rmqLogger) Fatalf(format string, v ...interface{}) { logger.Error().Msg(fmt.Sprintf(format, v...)) }
func (rmqLogger) Errorf(format string, v ...interface{}) { logger.Error().Msg(fmt.Sprintf(format, v...)) }
func (rmqLogger) Warnf(format string, v ...interface{})  { logger.Warn().Msg(fmt.Sprintf(format, v...)) }
func (rmqLogger) Infof(format string, v ...interface{})  { logger.Info().Msg(fmt.Sprintf(format, v...)) }
func (rmqLogger) Debugf(format string, v ...interface{}) { logger.Debug().Msg(fmt.Sprintf(format, v...)) }

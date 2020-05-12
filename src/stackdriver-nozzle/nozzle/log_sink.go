/*
 * Copyright 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nozzle

import (
	"encoding/json"
	"strings"

	"cloud.google.com/go/logging"
	"code.cloudfoundry.org/lager"
	"github.com/cloudfoundry-community/stackdriver-tools/src/stackdriver-nozzle/messages"
	"github.com/cloudfoundry-community/stackdriver-tools/src/stackdriver-nozzle/stackdriver"
	"github.com/cloudfoundry/sonde-go/events"
)

// NewLogSink returns a Sink that can receive sonde Events, translate them and send them to a stackdriver.LogAdapter
func NewLogSink(labelMaker LabelMaker, logAdapter stackdriver.LogAdapter, newlineToken string, logger lager.Logger) Sink {
	return &logSink{
		labelMaker:   labelMaker,
		logAdapter:   logAdapter,
		newlineToken: newlineToken,
		logger:       logger,
	}
}

type logSink struct {
	labelMaker   LabelMaker
	logAdapter   stackdriver.LogAdapter
	newlineToken string
	logger       lager.Logger
}

func (ls *logSink) Receive(envelope *events.Envelope) {
	if envelope == nil {
		// This happens when we get a fatal error from firehose,
		// It also happens a few thousand times in a row.
		// Quietly ignore the error and let other parts of the system handle the logging.
		return
	}
	log := ls.parseEnvelope(envelope)
	ls.logAdapter.PostLog(&log)
}

func structToMap(obj interface{}) (map[string]interface{}, error) {
	payloadJSON, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}

	var unmarshaledMap map[string]interface{}
	if err := json.Unmarshal(payloadJSON, &unmarshaledMap); err != nil {
		return nil, err
	}

	return unmarshaledMap, nil
}

func (ls *logSink) parseEnvelope(envelope *events.Envelope) messages.Log {
	payload, err := structToMap(envelope) // better hope that's json
	if err != nil {
		ls.logger.Error("error parsing envelope", err)
	}

	payload["eventType"] = envelope.GetEventType().String()

	severity := logging.Default

	// The json marshaling causes a loss in precision
	if envelope.GetTimestamp() != 0 {
		payload["timestamp"] = envelope.GetTimestamp()
	}

	switch envelope.GetEventType() {
	case events.Envelope_LogMessage:
		logMessage := envelope.GetLogMessage()
		logMessageMap, err := structToMap(logMessage)
		if err != nil {
			ls.logger.Error("error parsing logMessage", err)
		} else {
			rawMessage := logMessage.GetMessage()
			message := ls.parseMessage(rawMessage)
			var js map[string]interface{}
			err = json.Unmarshal([]byte(rawMessage), &js)
			if err == nil {
				if str, ok := js["msg"].(string); ok {
				  message = str
				  delete(js, "msg")
				}

				for key, element := range js {
					logMessageMap[key] = element
				}
			}
			// This is snake_cased to match the field in the protobuf. The other
			// fields we pass to Stackdriver are camelCased. We arbitrarily chose
			// to remain consistent with the protobuf.
			logMessageMap["message_type"] = logMessage.GetMessageType().String()
			severity = parseSeverity(logMessage.GetMessageType())

			// Put the message payload where stackdriver expects it
			payload["message"] = message

			// Avoid duplication of the message
			delete(logMessageMap, "message")

			payload["logMessage"] = logMessageMap
		}
	case events.Envelope_Error:
		errorMessage := envelope.GetError().GetMessage()
		payload["message"] = errorMessage
		severity = logging.Error
	case events.Envelope_HttpStartStop:
		httpStartStop := envelope.GetHttpStartStop()
		httpStartStopMap, err := structToMap(httpStartStop)
		if err != nil {
			ls.logger.Error("error parsing httpStartStop", err)
		} else {
			httpStartStopMap["method"] = httpStartStop.GetMethod().String()
			httpStartStopMap["peerType"] = httpStartStop.GetPeerType().String()
			httpStartStopMap["requestId"] = formatUUID(httpStartStop.GetRequestId())
			payload["httpStartStop"] = httpStartStopMap
		}
	}

	labels := ls.labelMaker.LogLabels(envelope)
	app := labels["applicationPath"]
	if app != "" {
		payload["serviceContext"] = map[string]interface{}{
			"service": app,
		}
	}

	log := messages.Log{
		Payload:  payload,
		Labels:   labels,
		Severity: severity,
	}

	return log
}

func (ls *logSink) parseMessage(rawMessage []byte) string {
	message := string(rawMessage)
	if ls.newlineToken != "" {
		message = strings.Replace(message, ls.newlineToken, "\n", -1)
	}
	return message
}

func parseSeverity(messageType events.LogMessage_MessageType) logging.Severity {
	if messageType == events.LogMessage_ERR {
		return logging.Error
	}

	return logging.Default
}

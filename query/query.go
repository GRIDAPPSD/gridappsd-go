// Package query sends typed requests for the platform's model data over the
// powergridmodel request destination and decodes the replies.
package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/GRIDAPPSD/gridappsd-go/topics"
)

const contentType = "application/json"

// Requester is the part of fieldbus.MessageBus these helpers use, so any
// MessageBus can be passed and tests can supply a small fake.
type Requester interface {
	GetResponse(ctx context.Context, destination, contentType string, body []byte) ([]byte, error)
}

// maxErrText bounds how much of a platform error member is quoted back.
const maxErrText = 200

var (
	errNotJSON    = errors.New("query: reply is not a JSON object")
	errNoData     = errors.New("query: reply has no data")
	errIncomplete = errors.New("query: reply says it is not complete")
)

// request sends body to the powergridmodel destination and returns the
// reply's data member as JSON.
func request(ctx context.Context, bus Requester, body string) (json.RawMessage, error) {
	reply, err := bus.GetResponse(ctx, topics.Blazegraph, contentType, []byte(body))
	if err != nil {
		return nil, fmt.Errorf("query: request failed: %w", err)
	}
	return decodeData(reply)
}

// decodeData accepts the two forms the platform uses for data: a JSON value,
// or a string holding JSON. A reply with an error member, with
// responseComplete false, or with no data is refused, so a caller never sees
// an empty success.
func decodeData(reply []byte) (json.RawMessage, error) {
	var env struct {
		Error            json.RawMessage `json:"error"`
		Data             json.RawMessage `json:"data"`
		ResponseComplete *bool           `json:"responseComplete"`
	}
	if err := json.Unmarshal(reply, &env); err != nil {
		return nil, errNotJSON
	}
	if hasErrorMember(env.Error) {
		return nil, fmt.Errorf("query: platform returned an error: %s", boundedText(env.Error))
	}
	if env.ResponseComplete != nil && !*env.ResponseComplete {
		return nil, errIncomplete
	}
	data := strings.TrimSpace(string(env.Data))
	if data == "" || data == "null" {
		return nil, errNoData
	}
	if data[0] == '"' {
		var inner string
		if err := json.Unmarshal(env.Data, &inner); err != nil {
			return nil, errNotJSON
		}
		if !json.Valid([]byte(inner)) {
			return nil, errors.New("query: data string does not hold JSON")
		}
		return json.RawMessage(inner), nil
	}
	return env.Data, nil
}

func hasErrorMember(raw json.RawMessage) bool {
	s := strings.TrimSpace(string(raw))
	return s != "" && s != "null" && s != `""` && s != "{}"
}

func boundedText(raw json.RawMessage) string {
	s := string(raw)
	if len(s) > maxErrText {
		s = s[:maxErrText] + "..."
	}
	return fmt.Sprintf("%q", s)
}

const modelNamesBody = `{"requestType":"QUERY_MODEL_NAMES"}`

// ModelNames returns the model names the platform holds, in the order it
// returned them. An error reply, a reply without a name list, an empty list
// and an empty or non-string name are all errors.
func ModelNames(ctx context.Context, bus Requester) ([]string, error) {
	data, err := request(ctx, bus, modelNamesBody)
	if err != nil {
		return nil, err
	}
	var d struct {
		ModelNames []any `json:"modelNames"`
	}
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("query: model names: %w", errNotJSON)
	}
	if len(d.ModelNames) == 0 {
		return nil, errors.New("query: reply holds no model names")
	}
	names := make([]string, 0, len(d.ModelNames))
	for i, v := range d.ModelNames {
		s, ok := v.(string)
		if !ok || s == "" {
			return nil, fmt.Errorf("query: model name %d is not a non-empty string", i)
		}
		names = append(names, s)
	}
	return names, nil
}

const modelInfoBody = `{"requestType":"QUERY_MODEL_INFO"}`

// Model is one model the platform holds. Both fields are exactly as stored:
// the mRID is matched byte for byte when it is used in a later query, so it
// is never case-folded or trimmed.
type Model struct {
	Name string
	MRID string
}

// ModelInfo returns each model's name and mRID in the order the platform
// returned them. An error reply, a reply without a model list, an empty
// list, and an entry with a missing or empty name or mRID are all errors.
func ModelInfo(ctx context.Context, bus Requester) ([]Model, error) {
	data, err := request(ctx, bus, modelInfoBody)
	if err != nil {
		return nil, err
	}
	var d struct {
		Models []map[string]any `json:"models"`
	}
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("query: model info: %w", errNotJSON)
	}
	if len(d.Models) == 0 {
		return nil, errors.New("query: reply holds no models")
	}
	models := make([]Model, 0, len(d.Models))
	for i, m := range d.Models {
		name, nameOK := m["modelName"].(string)
		mrid, mridOK := m["modelId"].(string)
		if !nameOK || !mridOK || name == "" || mrid == "" {
			return nil, fmt.Errorf("query: model %d lacks a non-empty modelName and modelId", i)
		}
		models = append(models, Model{Name: name, MRID: mrid})
	}
	return models, nil
}

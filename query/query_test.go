package query_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/GRIDAPPSD/gridappsd-go/internal/transporttest"
	"github.com/GRIDAPPSD/gridappsd-go/query"
)

const wantDestination = "/queue/goss.gridappsd.process.request.data.powergridmodel"

func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return c
}

func TestModelNamesRequest(t *testing.T) {
	t.Parallel()
	bus := transporttest.NewReplyBus([]byte(`{"data":{"modelNames":["feeder-a"]}}`))
	if _, err := query.ModelNames(ctx(t), bus); err != nil {
		t.Fatalf("ModelNames: %v", err)
	}
	sent, err := bus.Conn.LastSend()
	if err != nil {
		t.Fatal(err)
	}
	if sent.Destination != wantDestination {
		t.Errorf("destination = %q, want %q", sent.Destination, wantDestination)
	}
	if sent.ContentType != "application/json" {
		t.Errorf("content type = %q", sent.ContentType)
	}
	var got map[string]any
	if err := json.Unmarshal(sent.Body, &got); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	want := map[string]any{"requestType": "QUERY_MODEL_NAMES"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("body = %v, want exactly %v", got, want)
	}
}

func TestModelNamesReturnsNamesAsSent(t *testing.T) {
	t.Parallel()
	want := []string{"Feeder-B", "feeder-a", "FEEDER-C"}
	for name, reply := range map[string]string{
		"data as object": `{"data":{"modelNames":["Feeder-B","feeder-a","FEEDER-C"]}}`,
		"data as string": `{"data":"{\"modelNames\":[\"Feeder-B\",\"feeder-a\",\"FEEDER-C\"]}"}`,
		"complete":       `{"responseComplete":true,"data":{"modelNames":["Feeder-B","feeder-a","FEEDER-C"]}}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, err := query.ModelNames(ctx(t), transporttest.NewReplyBus([]byte(reply)))
			if err != nil {
				t.Fatalf("ModelNames: %v", err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("names = %q, want %q (case and order as sent)", got, want)
			}
		})
	}
}

func TestModelNamesRefusesBadReplies(t *testing.T) {
	t.Parallel()
	for name, reply := range map[string]string{
		"error member":           `{"error":{"message":"boom"},"data":{"modelNames":["feeder-a"]}}`,
		"error string":           `{"error":"boom"}`,
		"not JSON":               `not json`,
		"JSON but not an object": `["feeder-a"]`,
		"no data":                `{"responseComplete":true}`,
		"null data":              `{"data":null}`,
		"data string not JSON":   `{"data":"not json"}`,
		"no modelNames":          `{"data":{"models":[]}}`,
		"empty list":             `{"data":{"modelNames":[]}}`,
		"non-string element":     `{"data":{"modelNames":["feeder-a",7]}}`,
		"empty name":             `{"data":{"modelNames":["feeder-a",""]}}`,
		"marked incomplete":      `{"responseComplete":false,"data":{"modelNames":["feeder-a"]}}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			names, err := query.ModelNames(ctx(t), transporttest.NewReplyBus([]byte(reply)))
			if err == nil {
				t.Fatalf("ModelNames = %q, nil error; want an error", names)
			}
			if names != nil {
				t.Errorf("names = %q alongside an error", names)
			}
		})
	}
}

type failingBus struct{ err error }

func (b failingBus) GetResponse(context.Context, string, string, []byte) ([]byte, error) {
	return nil, b.err
}

func TestModelNamesWrapsTransportError(t *testing.T) {
	t.Parallel()
	cause := errors.New("no broker")
	_, err := query.ModelNames(ctx(t), failingBus{cause})
	if !errors.Is(err, cause) {
		t.Errorf("err = %v, want it to wrap %v", err, cause)
	}
}

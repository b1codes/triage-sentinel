package ingest

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// pubsubStub emulates the Pub/Sub REST surface.
type pubsubStub struct {
	pullStatus int
	pullBody   string
	ackStatus  int
	ackIDsSeen []string
	pullCalls  int
}

func (p *pubsubStub) handler(t *testing.T) http.Handler {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("/v1/projects/x/subscriptions/sub:pull", func(w http.ResponseWriter, r *http.Request) {
		p.pullCalls++
		if p.pullStatus != 0 && p.pullStatus != http.StatusOK {
			w.WriteHeader(p.pullStatus)
			_, _ = w.Write([]byte(`{"error":{"message":"boom"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(p.pullBody))
	})

	mux.HandleFunc("/v1/projects/x/subscriptions/sub:acknowledge", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			AckIDs []string `json:"ackIds"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decoding ack body: %v", err)
		}
		p.ackIDsSeen = append(p.ackIDsSeen, body.AckIDs...)
		if p.ackStatus != 0 && p.ackStatus != http.StatusOK {
			w.WriteHeader(p.ackStatus)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	})

	return mux
}

func testPuller(t *testing.T, stub *pubsubStub) (*RESTPuller, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(stub.handler(t))
	t.Cleanup(srv.Close)

	p, err := NewRESTPuller(RESTOptions{
		Subscription: "projects/x/subscriptions/sub",
		Client:       srv.Client(),
		BaseURL:      srv.URL,
	})
	if err != nil {
		t.Fatalf("NewRESTPuller() error = %v, want nil", err)
	}
	return p, srv
}

func TestRESTPullerPull(t *testing.T) {
	data := base64.StdEncoding.EncodeToString([]byte(`{"hello":"world"}`))
	stub := &pubsubStub{pullBody: `{"receivedMessages":[{
		"ackId":"ack-1",
		"message":{
			"messageId":"m-1",
			"data":"` + data + `",
			"attributes":{"x-github-event":"workflow_run"},
			"publishTime":"2026-08-02T11:04:05Z"
		}
	}]}`}

	p, _ := testPuller(t, stub)

	msgs, err := p.Pull(context.Background(), 10)
	if err != nil {
		t.Fatalf("Pull() error = %v, want nil", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d, want 1", len(msgs))
	}

	m := msgs[0]
	if m.ID != "m-1" {
		t.Errorf("ID = %q, want %q", m.ID, "m-1")
	}
	if m.AckID != "ack-1" {
		t.Errorf("AckID = %q, want %q", m.AckID, "ack-1")
	}
	if string(m.Data) != `{"hello":"world"}` {
		t.Errorf("Data = %q, want the base64-decoded body", string(m.Data))
	}
	if m.Attributes["x-github-event"] != "workflow_run" {
		t.Errorf("Attributes = %v, want the forwarded headers", m.Attributes)
	}
	if m.PublishTime.IsZero() {
		t.Error("PublishTime is zero")
	}
}

func TestRESTPullerEmptyPullIsNotAnError(t *testing.T) {
	p, _ := testPuller(t, &pubsubStub{pullBody: `{}`})

	msgs, err := p.Pull(context.Background(), 10)
	if err != nil {
		t.Fatalf("Pull() error = %v, want nil; an empty long-poll is normal", err)
	}
	if len(msgs) != 0 {
		t.Errorf("len(msgs) = %d, want 0", len(msgs))
	}
}

func TestRESTPullerErrors(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{name: "server error", status: http.StatusInternalServerError},
		{name: "unauthorised", status: http.StatusUnauthorized},
		{name: "rate limited", status: http.StatusTooManyRequests},
		{name: "malformed json", status: http.StatusOK, body: `{"receivedMessages":`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := testPuller(t, &pubsubStub{pullStatus: tc.status, pullBody: tc.body})
			_, err := p.Pull(context.Background(), 10)
			if !errors.Is(err, ErrPull) {
				t.Errorf("Pull() error = %v, want ErrPull", err)
			}
		})
	}
}

func TestRESTPullerUndecodableDataIsSkippedNotFatal(t *testing.T) {
	stub := &pubsubStub{pullBody: `{"receivedMessages":[
		{"ackId":"bad","message":{"messageId":"m-bad","data":"!!!not-base64!!!"}},
		{"ackId":"good","message":{"messageId":"m-good","data":"e30="}}
	]}`}
	p, _ := testPuller(t, stub)

	msgs, err := p.Pull(context.Background(), 10)
	if err != nil {
		t.Fatalf("Pull() error = %v, want nil; one bad message must not discard the batch", err)
	}
	if len(msgs) != 1 || msgs[0].ID != "m-good" {
		t.Errorf("msgs = %v, want only the decodable message", msgs)
	}
}

func TestRESTPullerAck(t *testing.T) {
	stub := &pubsubStub{}
	p, _ := testPuller(t, stub)

	if err := p.Ack(context.Background(), []string{"a", "b"}); err != nil {
		t.Fatalf("Ack() error = %v, want nil", err)
	}
	if len(stub.ackIDsSeen) != 2 {
		t.Errorf("ackIDsSeen = %v, want two ids", stub.ackIDsSeen)
	}

	t.Run("empty ack list makes no request", func(t *testing.T) {
		before := len(stub.ackIDsSeen)
		if err := p.Ack(context.Background(), nil); err != nil {
			t.Fatalf("Ack(nil) error = %v, want nil", err)
		}
		if len(stub.ackIDsSeen) != before {
			t.Error("Ack(nil) issued a request")
		}
	})

	t.Run("ack failure is reported", func(t *testing.T) {
		stub.ackStatus = http.StatusInternalServerError
		if err := p.Ack(context.Background(), []string{"c"}); !errors.Is(err, ErrPull) {
			t.Errorf("Ack() error = %v, want ErrPull", err)
		}
	})
}

func TestNewRESTPullerValidates(t *testing.T) {
	tests := []struct {
		name string
		opts RESTOptions
	}{
		{name: "empty subscription", opts: RESTOptions{}},
		{name: "bare subscription id", opts: RESTOptions{Subscription: "sub"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewRESTPuller(tc.opts); err == nil {
				t.Error("NewRESTPuller() error = nil, want error")
			}
		})
	}
}

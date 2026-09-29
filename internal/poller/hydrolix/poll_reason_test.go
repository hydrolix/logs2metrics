package hydrolix

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Every error poll carries a reason, so a destination that can't call
// /healthz can still alert on auth failures separately from bad SQL or an
// unreachable cluster.
func TestPollErrorsAreTaggedWithReason(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		body       string
		unreach    bool
		wantStatus string
		wantReason string // "" means the reason tag must be absent
	}{
		{name: "rejected token 400/516", status: http.StatusBadRequest, body: authFailedBody, wantStatus: "error", wantReason: "auth"},
		{name: "401", status: http.StatusUnauthorized, wantStatus: "error", wantReason: "auth"},
		{name: "403", status: http.StatusForbidden, wantStatus: "error", wantReason: "auth"},
		{name: "bad SQL 400/62", status: http.StatusBadRequest, body: syntaxErrBody, wantStatus: "error", wantReason: "query"},
		{name: "server error", status: http.StatusInternalServerError, body: "boom", wantStatus: "error", wantReason: "query"},
		{name: "unreachable host", unreach: true, wantStatus: "error", wantReason: "transport"},
		{name: "unparseable response", status: http.StatusOK, body: "<html>proxy error</html>", wantStatus: "error", wantReason: "decode"},
		{name: "success", status: http.StatusOK, body: `{"data":[]}`, wantStatus: "success"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			})
			srv := httptest.NewTLSServer(mux)
			host := srv.Listener.Addr().String()
			if tc.unreach {
				srv.Close() // nothing listens on host any more
			} else {
				defer srv.Close()
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sink := newCaptureSink()
			c := &Client{
				opts:       HydrolixOpts{Host: host},
				httpClient: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}},
				ctx:        ctx,
				config:     &QueriesConfig{},
				selfSink:   sink,
			}

			c.pollQuery(&QueryConfig{Name: "q1", renderedSQL: "select 1"})

			var polls []metricCapture
			for _, m := range sink.store.incs {
				if m.name == "hydrolix.collector.poll" {
					polls = append(polls, m)
				}
			}
			if len(polls) != 1 {
				t.Fatalf("want one hydrolix.collector.poll count, got %d: %+v", len(polls), polls)
			}
			tags := polls[0].tags
			if tags["status"] != tc.wantStatus {
				t.Errorf("status = %q, want %q", tags["status"], tc.wantStatus)
			}
			reason, has := tags["reason"]
			switch {
			case tc.wantReason == "" && has:
				t.Errorf("success poll carries reason=%q, want no reason tag", reason)
			case tc.wantReason != "" && reason != tc.wantReason:
				t.Errorf("reason = %q, want %q", reason, tc.wantReason)
			}
			if tags["query"] != "q1" {
				t.Errorf("query tag = %q, want q1", tags["query"])
			}
		})
	}
}

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

// With user/pass credentials a rejected token triggers a re-login, which
// fails in two further shapes: the login itself fails, or the freshly issued
// token is rejected too. Both are still auth failures.
func TestReloginFailuresAreTaggedAuth(t *testing.T) {
	cases := []struct {
		name  string
		setup func(a *authTestServer)
	}{
		{name: "re-login fails", setup: func(a *authTestServer) {
			a.rejectToken.Store("token-1")
			a.loginStatus.Store(http.StatusUnauthorized)
		}},
		{name: "fresh token also rejected", setup: func(a *authTestServer) {
			a.rejectAll.Store(true)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := newAuthTestServer(t)
			setAuthEnv(t, a.host(), "", "user", "pass")
			c := New("test", newOpts())
			if c == nil {
				t.Fatal("New returned nil")
			}
			defer c.cancel()
			sink := newCaptureSink()
			c.selfSink = sink
			tc.setup(a)

			if !c.pollQuery(&QueryConfig{Name: "q1", renderedSQL: "select 1"}) {
				t.Error("pollQuery should report an auth failure")
			}
			for _, m := range sink.store.incs {
				if m.name != "hydrolix.collector.poll" {
					continue
				}
				if m.tags["status"] != "error" || m.tags["reason"] != "auth" {
					t.Errorf("poll tags = %v, want status=error reason=auth", m.tags)
				}
				return
			}
			t.Fatal("no hydrolix.collector.poll count recorded")
		})
	}
}

// Stop cancels the client's context while a poll may be in flight. That
// request then fails with "context canceled", which is the collector
// shutting down, not the cluster being unreachable: it must not be counted
// as an error poll (reason=transport would fire on every restart).
func TestPollCancelledByShutdownIsNotCounted(t *testing.T) {
	arrived, release := make(chan struct{}), make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		close(arrived)
		select { // hold the request until the client gives up
		case <-r.Context().Done():
		case <-release:
		}
	})
	srv := httptest.NewTLSServer(mux)
	defer srv.Close()
	defer close(release) // runs first: frees the handler so Close can return

	ctx, cancel := context.WithCancel(context.Background())
	sink := newCaptureSink()
	c := &Client{
		opts:       HydrolixOpts{Host: srv.Listener.Addr().String()},
		httpClient: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}},
		ctx:        ctx,
		config:     &QueriesConfig{},
		selfSink:   sink,
	}

	done := make(chan bool)
	go func() { done <- c.pollQuery(&QueryConfig{Name: "q1", renderedSQL: "select 1"}) }()
	<-arrived
	cancel()
	if authFailed := <-done; authFailed {
		t.Error("a cancelled poll must not report an auth failure")
	}

	for _, m := range sink.store.incs {
		if m.name == "hydrolix.collector.poll" {
			t.Fatalf("a poll cancelled by shutdown was counted: %v", m.tags)
		}
	}
}

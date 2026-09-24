package hydrolix

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// authTestServer is a TLS Hydrolix stub that counts logins, hands out
// numbered tokens, and records the Authorization header of every query.
type authTestServer struct {
	srv         *httptest.Server
	logins      atomic.Int64
	loginStatus atomic.Int64 // response code for /config/v1/login/ (default 200)
	queryAuth   chan string  // Authorization header seen by /query
	rejectToken atomic.Value // string; queries carrying this token are rejected
	rejectWith  atomic.Int64 // status for rejected tokens: 400 (qe-3's real response, default) or 401
}

// Bodies as returned by qe-innovations-3 on /query (captured 2026-09-24,
// trimmed). A rejected bearer token is a 400 carrying ClickHouse code 516,
// not a 401.
const (
	authFailedBody = `{"error": "Code: 516. DB::Exception: : Authentication failed: password is incorrect, or there is no user with such name. (AUTHENTICATION_FAILED)", "query": "select 1"}`
	syntaxErrBody  = `{"error": "Code: 62. DB::Exception: Syntax error: failed at position 1 (broken): broken. Expected one of: Query, Query with output, EXPLAIN, SELECT query. (SYNTAX_ERROR)", "query": "broken"}`
)

func newAuthTestServer(t *testing.T) *authTestServer {
	t.Helper()
	a := &authTestServer{queryAuth: make(chan string, 64)}
	a.loginStatus.Store(http.StatusOK)
	a.rejectToken.Store("")
	a.rejectWith.Store(http.StatusBadRequest)

	mux := http.NewServeMux()
	mux.HandleFunc("/config/v1/login/", func(w http.ResponseWriter, r *http.Request) {
		n := a.logins.Add(1)
		if s := a.loginStatus.Load(); s != http.StatusOK {
			w.WriteHeader(int(s))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		// Real Hydrolix clusters nest the token: {"auth_token":{"access_token":...}}
		_, _ = fmt.Fprintf(w, `{"auth_token":{"access_token":"token-%d"}}`, n)
	})
	mux.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		a.queryAuth <- auth
		if bad := a.rejectToken.Load().(string); bad != "" && auth == "Bearer "+bad {
			if a.rejectWith.Load() == http.StatusUnauthorized {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(authFailedBody))
			return
		}
		if sql, _ := io.ReadAll(r.Body); strings.Contains(string(sql), "broken") {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(syntaxErrBody))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[]}`))
	})

	a.srv = httptest.NewTLSServer(mux)
	t.Cleanup(a.srv.Close)

	// New() builds its own http.Client with a nil Transport, which falls back
	// to http.DefaultTransport - point that at a TLS config trusting the stub.
	orig := http.DefaultTransport
	if tr, ok := orig.(*http.Transport); ok {
		clone := tr.Clone()
		clone.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		http.DefaultTransport = clone
		t.Cleanup(func() { http.DefaultTransport = orig })
	}
	return a
}

func (a *authTestServer) host() string { return a.srv.Listener.Addr().String() }

// setAuthEnv points the env-var branch at the stub. Empty values read as
// unset by common.GetEnvValue.
func setAuthEnv(t *testing.T, host, token, user, pass string) {
	t.Helper()
	t.Setenv("HDX_HOST", host)
	t.Setenv("HDX_TOKEN", token)
	t.Setenv("HDX_USERNAME", user)
	t.Setenv("HDX_PASSWORD", pass)
}

func newOpts() HydrolixOpts {
	return HydrolixOpts{ConfigPath: "", EmbeddedFS: testConfigsFS()}
}

// The env-var user/pass branch is the only one the CLI can reach, and it must
// actually log in: today it reads the variables and then queries with an
// empty bearer token.
func TestNewEnvUserPassLogsInAndQueriesWithToken(t *testing.T) {
	a := newAuthTestServer(t)
	setAuthEnv(t, a.host(), "", "user", "pass")

	c := New("test", newOpts())
	if c == nil {
		t.Fatal("New returned nil for valid env user/pass")
	}
	defer c.cancel()

	if got := a.logins.Load(); got != 1 {
		t.Fatalf("expected exactly one login at construction, got %d", got)
	}
	if _, err := c.Query("test_query", "select 1"); err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if auth := <-a.queryAuth; auth != "Bearer token-1" {
		t.Fatalf("query sent %q, want %q", auth, "Bearer token-1")
	}
}

// Stop must wait for the token-refresh goroutine and return promptly. It
// waits up to 10s before giving up, so a Done that never fires shows up as a
// slow Stop, and a doubled Done panics.
func TestStopWaitsForTokenRefresh(t *testing.T) {
	a := newAuthTestServer(t)
	setAuthEnv(t, a.host(), "", "user", "pass")

	c := New("test", newOpts())
	if c == nil {
		t.Fatal("New returned nil for valid env user/pass")
	}

	done := make(chan struct{})
	go func() {
		c.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return within 5s; the refresh goroutine was not waited for")
	}
}

// A failed initial login must fail construction, not log-and-continue with an
// empty token that 401s forever.
func TestNewEnvUserPassFailsLoudlyWhenLoginFails(t *testing.T) {
	a := newAuthTestServer(t)
	a.loginStatus.Store(http.StatusInternalServerError)
	setAuthEnv(t, a.host(), "", "user", "pass")

	if c := New("test", newOpts()); c != nil {
		c.cancel()
		t.Fatal("New should return nil when the initial login fails")
	}
}

// A missing password must fail construction the same way a missing username
// does; today it is logged and ignored.
func TestNewEnvUserWithoutPasswordFails(t *testing.T) {
	a := newAuthTestServer(t)
	setAuthEnv(t, a.host(), "", "user", "")

	if c := New("test", newOpts()); c != nil {
		c.cancel()
		t.Fatal("New should return nil when HDX_PASSWORD is missing")
	}
}

// The opts-provided user/pass branch fetched a token and then overwrote it
// with the empty opts.Token ("c.token = o.Token").
func TestNewOptsUserPassKeepsFetchedToken(t *testing.T) {
	a := newAuthTestServer(t)
	setAuthEnv(t, "", "", "", "")

	o := newOpts()
	o.Host = a.host()
	o.Username = "user"
	o.Password = "pass"

	c := New("test", o)
	if c == nil {
		t.Fatal("New returned nil for valid opts user/pass")
	}
	defer c.cancel()

	c.mu.RLock()
	token := c.token
	c.mu.RUnlock()
	if token != "token-1" {
		t.Fatalf("fetched token was clobbered: got %q, want %q", token, "token-1")
	}
}

// In user/pass mode a rejected token means it expired or was invalidated:
// re-login once and retry. The stub rejects it the way qe-3 does (400 +
// AUTHENTICATION_FAILED), not with a 401.
func TestQueryReloginOnRejectedToken(t *testing.T) {
	a := newAuthTestServer(t)
	setAuthEnv(t, a.host(), "", "user", "pass")

	c := New("test", newOpts())
	if c == nil {
		t.Fatal("New returned nil")
	}
	defer c.cancel()
	a.rejectToken.Store("token-1") // simulate expiry of the initial token

	if _, err := c.Query("test_query", "select 1"); err != nil {
		t.Fatalf("Query should recover from a rejected token via re-login, got: %v", err)
	}
	if got := a.logins.Load(); got != 2 {
		t.Fatalf("expected initial login + one re-login, got %d logins", got)
	}
	if first := <-a.queryAuth; first != "Bearer token-1" {
		t.Fatalf("first attempt sent %q", first)
	}
	if retry := <-a.queryAuth; retry != "Bearer token-2" {
		t.Fatalf("retry sent %q, want the refreshed token", retry)
	}
}

// With a static token there is nothing to refresh: a 401 must be a hard,
// clearly-worded error and must not attempt a login.
func TestQuery401WithStaticTokenIsHardError(t *testing.T) {
	a := newAuthTestServer(t)
	a.rejectToken.Store("static-token")
	setAuthEnv(t, a.host(), "static-token", "", "")

	c := New("test", newOpts())
	if c == nil {
		t.Fatal("New returned nil for token auth")
	}
	defer c.cancel()

	_, err := c.Query("test_query", "select 1")
	if err == nil {
		t.Fatal("expected an error for a rejected static token")
	}
	if !strings.Contains(err.Error(), "static token was rejected") {
		t.Fatalf("error should say the static token was rejected, got: %v", err)
	}
	var qerr *QueryError
	if !errors.As(err, &qerr) {
		t.Fatalf("error should wrap the server's *QueryError, got: %v", err)
	}
	if got := a.logins.Load(); got != 0 {
		t.Fatalf("static-token mode must not attempt a login, got %d", got)
	}
}

// The stampede guard: if another goroutine already refreshed the token, a
// 401-holder must reuse the new token instead of logging in again.
func TestReloginAfter401SkipsWhenTokenAlreadyRefreshed(t *testing.T) {
	a := newAuthTestServer(t)
	setAuthEnv(t, a.host(), "", "user", "pass")

	c := New("test", newOpts())
	if c == nil {
		t.Fatal("New returned nil")
	}
	defer c.cancel()
	loginsAfterNew := a.logins.Load()

	// Simulate a concurrent refresh having replaced the token already.
	c.mu.Lock()
	c.token = "token-fresh"
	c.mu.Unlock()

	if err := c.reloginAfter401(c.ctx, "token-1"); err != nil {
		t.Fatalf("reloginAfter401 failed: %v", err)
	}
	if got := a.logins.Load(); got != loginsAfterNew {
		t.Fatalf("guard should skip the login, got %d extra", got-loginsAfterNew)
	}
}

// A password that stops working must not cost one failed login per rejected
// query: concurrent 401s after a failed login reuse that failure, and every
// caller still gets a 401 *QueryError for health checks.
func TestFailedReloginIsRateLimited(t *testing.T) {
	a := newAuthTestServer(t)
	setAuthEnv(t, a.host(), "", "user", "pass")

	c := New("test", newOpts())
	if c == nil {
		t.Fatal("New returned nil")
	}
	defer c.cancel()

	a.rejectToken.Store("token-1")               // the current token is rejected
	a.loginStatus.Store(http.StatusUnauthorized) // and the password no longer works
	loginsBefore := a.logins.Load()

	const queries = 5
	errs := make([]error, queries)
	var wg sync.WaitGroup
	for i := range queries {
		wg.Go(func() { _, errs[i] = c.Query("test_query", "select 1") })
	}
	wg.Wait()

	if got := a.logins.Load() - loginsBefore; got != 1 {
		t.Fatalf("expected 1 login attempt for %d concurrent 401s, got %d", queries, got)
	}
	for i, err := range errs {
		var qerr *QueryError
		if !errors.As(err, &qerr) {
			t.Errorf("query %d: error should wrap the server's *QueryError, got: %v", i, err)
		}
	}
}

// Once the cooldown has passed, the next 401 tries to log in again, so the
// collector recovers by itself when the password is fixed.
func TestReloginRetriesAfterCooldown(t *testing.T) {
	orig := reloginCooldown
	reloginCooldown = 50 * time.Millisecond
	t.Cleanup(func() { reloginCooldown = orig })

	a := newAuthTestServer(t)
	setAuthEnv(t, a.host(), "", "user", "pass")

	c := New("test", newOpts())
	if c == nil {
		t.Fatal("New returned nil")
	}
	defer c.cancel()

	a.rejectToken.Store("token-1")
	a.loginStatus.Store(http.StatusUnauthorized)
	loginsBefore := a.logins.Load()

	if _, err := c.Query("test_query", "select 1"); err == nil {
		t.Fatal("expected an error while the password is rejected")
	}
	if _, err := c.Query("test_query", "select 1"); err == nil {
		t.Fatal("expected an error within the cooldown")
	}
	if got := a.logins.Load() - loginsBefore; got != 1 {
		t.Fatalf("expected 1 login attempt within the cooldown, got %d", got)
	}

	a.loginStatus.Store(http.StatusOK) // password fixed
	time.Sleep(2 * reloginCooldown)
	if _, err := c.Query("test_query", "select 1"); err != nil {
		t.Fatalf("expected recovery after the cooldown, got: %v", err)
	}
	if got := a.logins.Load() - loginsBefore; got != 2 {
		t.Fatalf("expected a second login attempt after the cooldown, got %d total", got)
	}
}

// Sources can be mixed: a host given via opts must not prevent credentials
// from being read out of the environment.
func TestNewOptsHostWithEnvToken(t *testing.T) {
	a := newAuthTestServer(t)
	setAuthEnv(t, "", "env-token", "", "")

	o := newOpts()
	o.Host = a.host()

	c := New("test", o)
	if c == nil {
		t.Fatal("New returned nil for opts host + env token")
	}
	defer c.cancel()

	if _, err := c.Query("test_query", "select 1"); err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if auth := <-a.queryAuth; auth != "Bearer env-token" {
		t.Fatalf("query sent %q, want the env token", auth)
	}
}

// A gateway in front of a cluster may answer a rejected token with a plain
// 401; that must still trigger the re-login.
func TestQueryReloginOnPlain401(t *testing.T) {
	a := newAuthTestServer(t)
	a.rejectWith.Store(http.StatusUnauthorized)
	setAuthEnv(t, a.host(), "", "user", "pass")

	c := New("test", newOpts())
	if c == nil {
		t.Fatal("New returned nil")
	}
	defer c.cancel()
	a.rejectToken.Store("token-1")

	if _, err := c.Query("test_query", "select 1"); err != nil {
		t.Fatalf("Query should recover from a 401 via re-login, got: %v", err)
	}
	if got := a.logins.Load(); got != 2 {
		t.Fatalf("expected initial login + one re-login, got %d logins", got)
	}
}

// A 400 that is not an auth failure (bad SQL) must not trigger a re-login.
func TestQueryNonAuth400DoesNotRelogin(t *testing.T) {
	a := newAuthTestServer(t)
	setAuthEnv(t, a.host(), "", "user", "pass")

	c := New("test", newOpts())
	if c == nil {
		t.Fatal("New returned nil")
	}
	defer c.cancel()
	loginsBefore := a.logins.Load()

	_, err := c.Query("test_query", "broken")
	var qerr *QueryError
	if !errors.As(err, &qerr) || qerr.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected the 400 *QueryError back, got: %v", err)
	}
	if got := a.logins.Load() - loginsBefore; got != 0 {
		t.Fatalf("a syntax error must not trigger a re-login, got %d", got)
	}
}

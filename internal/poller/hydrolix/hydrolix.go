package hydrolix

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mercereau/hydrolix-metrics-go/internal/common"
	"github.com/mercereau/hydrolix-metrics-go/internal/sinks"
)

type Client struct {
	name               string
	token              string
	sinks              sinks.MetricSinks
	selfSink           sinks.MetricSink // scoped sink for collector self-metrics
	opts               HydrolixOpts
	config             *QueriesConfig
	userAgentBase      string
	queriesAreEmbedded bool
	httpClient         *http.Client

	mu      sync.RWMutex
	loginMu sync.Mutex // serializes re-logins triggered by 401 responses
	closeCh chan struct{}
	closed  bool
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	started                atomic.Bool // true while the poller loop (Start) is running
	lastRoundAllAuthFailed atomic.Bool // true if every query in the most recent poll round failed with an auth error
}

// QueryError represents a non-2xx response from the Hydrolix query endpoint.
type QueryError struct {
	StatusCode int
	Body       string
}

func (e *QueryError) Error() string {
	return fmt.Sprintf("query failed: status=%d body=%s", e.StatusCode, e.Body)
}

type HydrolixOpts struct {
	Host               string
	Username           string
	Password           string
	Token              string
	IntervalSeconds    time.Duration // Polling interval duration (e.g., 15 * time.Second)
	OffsetStartMinutes int           // How far back the query window starts (default 6)
	OffsetEndMinutes   int           // Lag offset for the query window end (default 1)
	ConfigPath         string        // Path to external queries.yaml (empty = use embedded defaults)
	EmbeddedFS         fs.FS         // Embedded configs filesystem from project root
}

type loginRequest struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

// loginResponse accepts both login shapes: real clusters nest the token under
// auth_token, while a bare access_token is kept for compatibility.
type loginResponse struct {
	AccessToken string `json:"access_token"`
	AuthToken   struct {
		AccessToken string `json:"access_token"`
	} `json:"auth_token"`
}

func (lr loginResponse) token() string {
	if lr.AuthToken.AccessToken != "" {
		return lr.AuthToken.AccessToken
	}
	return lr.AccessToken
}

func New(name string, o HydrolixOpts, ms ...sinks.MetricSink) *Client {
	// Load query config.
	cfg, err := LoadConfig(o.ConfigPath, o.EmbeddedFS)
	if err != nil {
		slog.Error("Failed to load query config", "error", err)
		return nil
	}

	// CLI offset flags override config defaults when non-zero.
	if o.OffsetStartMinutes != 0 {
		cfg.Defaults.OffsetStartMinutes = o.OffsetStartMinutes
		// Re-resolve queries with updated offsets.
		cfg, err = reResolveOffsets(cfg)
		if err != nil {
			slog.Error("Failed to re-resolve config with CLI offsets", "error", err)
			return nil
		}
	}
	if o.OffsetEndMinutes != 0 {
		cfg.Defaults.OffsetEndMinutes = o.OffsetEndMinutes
		cfg, err = reResolveOffsets(cfg)
		if err != nil {
			slog.Error("Failed to re-resolve config with CLI offsets", "error", err)
			return nil
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	// Release the context if construction fails below.
	built := false
	defer func() {
		if !built {
			cancel()
		}
	}()
	c := &Client{
		name:               name,
		opts:               o,
		config:             cfg,
		closeCh:            make(chan struct{}),
		sinks:              ms,
		httpClient:         &http.Client{Timeout: 30 * time.Second},
		userAgentBase:      newAdminCommentBase(o.IntervalSeconds, sinks.MetricSinks(ms).Name()),
		queriesAreEmbedded: o.ConfigPath == "",
		ctx:                ctx,
		cancel:             cancel,
	}
	c.selfSink = c.sinks.WithTags(sinks.Tags{"component": "hydrolix-collector"})

	// Resolve credentials per field: anything not supplied via opts falls
	// back to the environment, so sources can be mixed.
	if o.Host == "" {
		if host, err := common.GetEnvValue("HDX_HOST"); err == nil {
			o.Host = host
			slog.Info(fmt.Sprintf("Using host from environment variable HDX_HOST: %s", o.Host))
		}
	}
	if o.Token == "" && (o.Username == "" || o.Password == "") {
		var err error
		o.Token, err = common.GetEnvValue("HDX_TOKEN")
		if err == nil {
			slog.Info("Using token from environment variable HDX_TOKEN")
		} else {
			slog.Info(fmt.Sprintf("Unable to read HDX_TOKEN: %s", err.Error()))
			slog.Info("Falling back to username/password from environment variables")
			o.Username, err = common.GetEnvValue("HDX_USERNAME")
			if err != nil {
				slog.Error(err.Error())
				return nil
			}
			o.Password, err = common.GetEnvValue("HDX_PASSWORD")
			if err != nil {
				slog.Error(err.Error())
				return nil
			}
		}
	}

	if o.Host == "" {
		slog.Error("No Hydrolix host provided (set HDX_HOST)")
		return nil
	}
	c.opts = o

	// Authenticate. Both username/password sources (opts and environment)
	// take the same path: log in now, fail construction if that fails, and
	// keep the token fresh in the background.
	switch {
	case o.Token != "":
		slog.Info("Using token instead of username/password")
		c.token = o.Token
	case o.Username != "" && o.Password != "":
		slog.Info("Initializing client with username/password")
		if err := c.UpdateToken(context.Background()); err != nil {
			slog.Error("Initial login failed", "error", err)
			return nil
		}
		c.wg.Go(c.refreshToken)
	default:
		slog.Error("No authentication method provided for Hydrolix Client")
		return nil
	}
	built = true
	return c
}

// reResolveOffsets re-renders SQL templates for queries that inherit default offsets.
func reResolveOffsets(cfg *QueriesConfig) (*QueriesConfig, error) {
	for i := range cfg.Queries {
		q := &cfg.Queries[i]
		// Only update queries that inherit from defaults (no per-query override).
		if q.OffsetStartMinutes == nil {
			q.resolvedOffsetStart = cfg.Defaults.OffsetStartMinutes
		}
		if q.OffsetEndMinutes == nil {
			q.resolvedOffsetEnd = cfg.Defaults.OffsetEndMinutes
		}
		// Re-render SQL template.
		if err := renderQuerySQL(q); err != nil {
			return nil, err
		}
	}
	return cfg, nil
}

func (c *Client) refreshToken() {
	// A flat hourly cadence: tokens live much longer than a polling interval,
	// and tying the refresh to it caused a fresh login every poll (15s by
	// default). Expiry between refreshes is handled by the 401 path in Query.
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			slog.Info("Token refresh goroutine stopping")
			return
		case <-ticker.C:
			if err := c.UpdateToken(c.ctx); err != nil {
				slog.Warn("Token could not be refreshed", "error", err)
			} else {
				slog.Debug("Token refreshed successfully")
			}
		}
	}
}

func (c *Client) Register(ms sinks.MetricSinks) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sinks = ms
	return nil
}

func (c *Client) Start() {
	c.wg.Add(1)
	defer c.wg.Done()

	for _, m := range c.sinks {
		m.Start()
	}

	pollingInterval := c.opts.IntervalSeconds
	if pollingInterval <= 0 {
		pollingInterval = 15 * time.Second
		slog.Warn("Invalid polling interval, using default", "default", pollingInterval)
	}

	ticker := time.NewTicker(pollingInterval)
	defer ticker.Stop()

	slog.Info("Starting Hydrolix poller", "interval", pollingInterval, "queries", len(c.config.Queries))

	c.started.Store(true)
	defer c.started.Store(false)

	// Poll immediately on start.
	c.pollAll()

	for {
		select {
		case <-c.ctx.Done():
			slog.Info("Hydrolix poller stopping")
			return
		case <-ticker.C:
			c.pollAll()
		}
	}
}

// Healthy reports whether the collector is fit to serve traffic: the poller
// loop is running, and it isn't stuck in an auth-dead state where every query
// in the last round failed with an authentication/authorization error.
func (c *Client) Healthy() bool {
	return c.started.Load() && !c.lastRoundAllAuthFailed.Load()
}

// pollAll fans out all configured queries concurrently.
func (c *Client) pollAll() {
	n := len(c.config.Queries)
	if n == 0 {
		c.lastRoundAllAuthFailed.Store(false)
		return
	}

	var wg sync.WaitGroup
	authFailed := make([]bool, n)

	for i := range c.config.Queries {
		wg.Go(func() {
			authFailed[i] = c.pollQuery(&c.config.Queries[i])
		})
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-c.ctx.Done():
		slog.Debug("Context cancelled while waiting for polls to complete")
		return
	}

	allAuthFailed := true
	for _, f := range authFailed {
		if !f {
			allAuthFailed = false
			break
		}
	}
	c.lastRoundAllAuthFailed.Store(allAuthFailed)
}

// pollQuery executes a single configured query and emits metrics. It returns
// true if the query failed with an authentication/authorization error.
func (c *Client) pollQuery(q *QueryConfig) bool {
	select {
	case <-c.ctx.Done():
		slog.Debug("Skipping poll - context cancelled", "query", q.Name)
		return false
	default:
	}

	pollTags := sinks.Tags{"query": q.Name}
	stopTimer := sinks.StartTimer(c.selfSink, "hydrolix.collector.poll.duration", pollTags)
	defer stopTimer()

	slog.Debug("Polling query", "name", q.Name)

	body, err := c.Query(q.Name, q.RenderedSQL())
	if err != nil {
		slog.Error("Failed to query Hydrolix", "query", q.Name, "error", err)
		c.selfSink.Inc("hydrolix.collector.poll", "total", 1, sinks.MergeTags(pollTags, sinks.Tags{"status": "error"}))
		var qerr *QueryError
		return errors.As(err, &qerr) &&
			(qerr.StatusCode == http.StatusUnauthorized || qerr.StatusCode == http.StatusForbidden)
	}

	var resp GenericResponse
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		slog.Error("Failed to unmarshal response", "query", q.Name, "error", err)
		c.selfSink.Inc("hydrolix.collector.poll", "total", 1, sinks.MergeTags(pollTags, sinks.Tags{"status": "error"}))
		return false
	}

	c.selfSink.Inc("hydrolix.collector.poll", "total", 1, sinks.MergeTags(pollTags, sinks.Tags{"status": "success"}))
	c.selfSink.Gauge("hydrolix.collector.poll.rows", "row", float64(len(resp.Data)), pollTags)

	emitMetrics(q, &resp, c.config.Defaults.Tags, c.sinks)

	slog.Debug("Completed polling", "query", q.Name, "rows", len(resp.Data))
	return false
}

// Stop signals all listeners that we're shutting down.
func (c *Client) Stop() {
	slog.Info("Stopping Hydrolix client")
	c.mu.Lock()

	if c.closed {
		c.mu.Unlock()
		slog.Warn("Hydrolix client already stopped")
		return
	}
	c.closed = true
	c.mu.Unlock()

	c.cancel()

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("All goroutines stopped gracefully")
	case <-time.After(10 * time.Second):
		slog.Warn("Timeout waiting for goroutines to stop, forcing shutdown")
	}

	close(c.closeCh)

	for _, m := range c.sinks {
		m.Stop()
		slog.Debug("Stopped sink")
	}
	slog.Info("Hydrolix client stopped")
}

func (c *Client) done() <-chan struct{} {
	return c.closeCh
}

// UpdateToken logs in and swaps the client token. The HTTP call happens
// outside the lock so concurrent queries are not stalled behind a slow login;
// the lock is held only to swap the token in.
func (c *Client) UpdateToken(ctx context.Context) error {
	token, err := c.fetchToken(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.token = token
	c.mu.Unlock()
	return nil
}

func (c *Client) fetchToken(ctx context.Context) (string, error) {
	login := loginRequest{c.opts.Username, c.opts.Password}
	data, err := json.Marshal(login)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}

	url := fmt.Sprintf("https://%s/config/v1/login/", c.opts.Host)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("login failed: status=%d body=%s", resp.StatusCode, string(body))
	}

	var lr loginResponse
	if err := json.Unmarshal(body, &lr); err != nil {
		return "", fmt.Errorf("unmarshal login response: %w", err)
	}
	if lr.token() == "" {
		return "", fmt.Errorf("login response missing access_token")
	}

	return lr.token(), nil
}

// reloginAfter401 refreshes the token after a query was rejected with 401.
// Logins are serialized, and if another goroutine already replaced the token
// the caller just retries with that one - so a burst of concurrent 401s
// produces a single login, not a stampede.
func (c *Client) reloginAfter401(ctx context.Context, usedToken string) error {
	c.loginMu.Lock()
	defer c.loginMu.Unlock()
	if c.currentToken() != usedToken {
		return nil // someone else already refreshed it
	}
	slog.Warn("Query rejected with 401; re-logging in")
	return c.UpdateToken(ctx)
}

func (c *Client) currentToken() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.token
}

func (c *Client) Query(queryName, sql string) (string, error) {
	token := c.currentToken()
	status, body, err := c.doQuery(queryName, sql, token)
	if err != nil {
		return "", err
	}

	if status == http.StatusUnauthorized {
		// Both failures below wrap the 401 as a *QueryError so health checks
		// still recognise them as auth failures.
		qerr := &QueryError{StatusCode: status, Body: body}
		if c.opts.Username == "" || c.opts.Password == "" {
			return "", fmt.Errorf("the static token was rejected (invalid or expired) and the collector has no credentials to refresh it - provide a valid HDX_TOKEN, or HDX_USERNAME/HDX_PASSWORD: %w", qerr)
		}
		if err := c.reloginAfter401(c.ctx, token); err != nil {
			return "", fmt.Errorf("%w; re-login after 401 failed: %w", qerr, err)
		}
		status, body, err = c.doQuery(queryName, sql, c.currentToken())
		if err != nil {
			return "", err
		}
	}

	if status != http.StatusOK {
		return "", &QueryError{StatusCode: status, Body: body}
	}
	return body, nil
}

// doQuery performs one HTTP query attempt with the given token and returns
// the status code and body; only transport-level problems are errors.
func (c *Client) doQuery(queryName, sql, token string) (int, string, error) {
	url := fmt.Sprintf("https://%s/query?%s", c.opts.Host, url.Values{
		"hdx_query_admin_comment": {c.userAgentAdminComment(queryName)},
	}.Encode())

	req, err := http.NewRequestWithContext(c.ctx, http.MethodPost, url, strings.NewReader(sql))
	if err != nil {
		return 0, "", fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "text/plain")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, "", fmt.Errorf("read body: %w", err)
	}

	return resp.StatusCode, string(body), nil
}

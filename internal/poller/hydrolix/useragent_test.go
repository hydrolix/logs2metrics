package hydrolix

import (
	"strings"
	"testing"
	"time"

	"github.com/mercereau/hydrolix-metrics-go/internal/build"
	"github.com/mercereau/hydrolix-metrics-go/internal/sinks"
)

func TestUserAgentAdminCommentFormat(t *testing.T) {
	c := &Client{
		userAgentBase:      "User: " + productName + " version: v1.1.0 interval: 15s",
		queriesAreEmbedded: true,
	}
	got := c.userAgentAdminComment("akamai_errors")
	want := "User: hydrolix-metrics-go version: v1.1.0 interval: 15s query: akamai_errors"
	if got != want {
		t.Errorf("userAgentAdminComment() = %q, want %q", got, want)
	}
}

// A name from an operator's own config file is theirs, not ours to report.
func TestUserAgentAdminCommentOmitsCustomQueryNames(t *testing.T) {
	c := &Client{
		userAgentBase:      "User: " + productName,
		queriesAreEmbedded: false,
	}
	got := c.userAgentAdminComment("acmecorp_prod_customer_errors")
	if strings.Contains(got, "acmecorp") {
		t.Errorf("custom query name reached the admin comment: %q", got)
	}
	if !strings.HasSuffix(got, "query: custom") {
		t.Errorf("userAgentAdminComment() = %q, want it to end with %q", got, "query: custom")
	}
}

// The base describes the collector itself: what it is, which build it is, and
// how often it polls.
func TestAdminCommentBaseOnlyDescribesTheCollector(t *testing.T) {
	defer func(v string) { build.Version = v }(build.Version)

	build.Version = "v1.1.0-a2316f4"
	ms := sinks.MetricSinks{sinks.NewNop(nil), sinks.NewNop(nil)}
	got := newAdminCommentBase(30*time.Second, ms.Name())
	want := "User: hydrolix-metrics-go version: v1.1.0-a2316f4 interval: 30s sinks: nop,nop"
	if got != want {
		t.Errorf("newAdminCommentBase() = %q, want %q", got, want)
	}

	build.Version = ""
	got = newAdminCommentBase(0, sinks.MetricSinks{}.Name())
	for _, token := range []string{"version: dev", "sinks: none"} {
		if !strings.Contains(got, token) {
			t.Errorf("newAdminCommentBase() = %q, want it to report %q", got, token)
		}
	}
}

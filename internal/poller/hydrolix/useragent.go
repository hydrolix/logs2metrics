package hydrolix

import (
	"fmt"
	"strings"
	"time"

	"github.com/mercereau/hydrolix-metrics-go/internal/build"
)

const productName = "hydrolix-metrics-go"
const customQueryName = "custom"

// newAdminCommentBase builds the part of the comment that is fixed for the life
// of the process.
func newAdminCommentBase(interval time.Duration, sinkNames string) string {
	// Tokens are space-separated, so whitespace stamped into the version would
	// read as a token boundary. A build with no version stamped in reports "dev".
	version := strings.Join(strings.Fields(build.Version), "_")
	if version == "" {
		version = "dev"
	}
	if sinkNames == "" {
		sinkNames = "none"
	}
	return fmt.Sprintf("User: %s version: %s interval: %s sinks: %s", productName, version, interval, sinkNames)
}

// userAgentAdminComment returns the admin comment for a single poll. Each tick
// fires every configured query concurrently, so naming the query is what tells
// the requests apart in Hydrolix's query log.
func (c *Client) userAgentAdminComment(queryName string) string {
	if !c.queriesAreEmbedded {
		// Only the embedded config's query names are ours to report, and only
		// those are known to carry no spaces. An operator's own --config may
		// name queries after their company, team, or customers.
		queryName = customQueryName
	}
	return fmt.Sprintf("%s query: %s", c.userAgentBase, queryName)
}

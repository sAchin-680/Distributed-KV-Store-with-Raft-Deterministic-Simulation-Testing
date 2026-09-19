// Package buildinfo carries build metadata stamped in at link time.
//
// Values are set with -ldflags -X by the Makefile. They stay as "dev" and
// "none" in a plain `go build`, which is the honest answer for a binary nobody
// recorded the provenance of.
package buildinfo

import "fmt"

var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)

// String is a one-line summary suitable for a --version flag.
func String() string {
	return fmt.Sprintf("%s (commit %s, built %s)", Version, Commit, Date)
}

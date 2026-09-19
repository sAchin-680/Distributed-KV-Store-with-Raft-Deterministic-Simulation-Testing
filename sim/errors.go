package sim

import (
	"errors"

	"github.com/sAchin-680/raftkv/raft"
)

// asErr is errors.As with a friendlier shape for the one type this package
// unwraps.
func asErr(err error, target **Violation) bool { return errors.As(err, target) }

// isIgnored reports whether an error from Step is the core's way of saying it
// discarded a message on purpose. Stale, duplicated and inapplicable messages
// are ordinary network behaviour, not failures, and treating them as failures
// would make every run fail immediately.
func isIgnored(err error) bool { return errors.Is(err, raft.ErrIgnoredMessage) }

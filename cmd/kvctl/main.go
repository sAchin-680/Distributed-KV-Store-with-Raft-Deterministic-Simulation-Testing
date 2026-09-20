// Command kvctl is a client for a raftkv cluster.
//
//	kvctl --endpoints=localhost:8001,localhost:8002,localhost:8003 set foo bar
//	kvctl get foo
//	kvctl delete foo
//	kvctl status
//
// Endpoints are the whole cluster, not one node. Leadership moves on its own
// schedule, so a client that has to be told which node to talk to is broken by
// an ordinary election; this one follows the redirect and remembers where it
// ended up.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/sAchin-680/raftkv/internal/buildinfo"
	"github.com/sAchin-680/raftkv/kvserver"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "kvctl:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		endpoints = flag.String("endpoints", "localhost:8001",
			"comma-separated client addresses of the cluster")
		timeout = flag.Duration("timeout", 5*time.Second,
			"per-command timeout\n"+
				"\tA membership change is two commits rather than one, so it is\n"+
				"\tgiven longer than this by default.")
		stale = flag.Bool("stale", false,
			"read from whichever node answers, without confirming leadership\n"+
				"\tFast, and may return an arbitrarily out-of-date value.")
		noSession = flag.Bool("no-session", false,
			"skip registering a client session\n"+
				"\tWithout a session a retried write can be applied twice, which\n"+
				"\tleaves the map correct but the history not linearizable.")
		version = flag.Bool("version", false, "print the version and exit")
	)
	flag.Usage = usage
	flag.Parse()

	if *version {
		fmt.Println("kvctl", buildinfo.String())
		return nil
	}

	args := flag.Args()
	if len(args) == 0 {
		usage()
		return errors.New("a command is required")
	}

	client, err := kvserver.NewClient(splitEndpoints(*endpoints)...)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	commandTimeout := *timeout
	if args[0] == "members" {
		commandTimeout = max(commandTimeout, membershipTimeout)
	}
	ctx, cancel := context.WithTimeout(context.Background(), commandTimeout)
	defer cancel()

	// Writes register a session first, so a retry after a timeout is recognized
	// rather than applied a second time.
	needsSession := (args[0] == "set" || args[0] == "delete") && !*noSession
	if needsSession {
		if _, err := client.Register(ctx); err != nil {
			return fmt.Errorf("registering a client session: %w", err)
		}
	}

	switch args[0] {
	case "set":
		if len(args) != 3 {
			return errors.New("usage: kvctl set <key> <value>")
		}
		index, err := client.Set(ctx, []byte(args[1]), []byte(args[2]))
		if err != nil {
			return describe(err)
		}
		fmt.Printf("committed at index %d\n", index)

	case "get":
		if len(args) != 2 {
			return errors.New("usage: kvctl get <key>")
		}
		var (
			value []byte
			found bool
		)
		if *stale {
			value, found, err = client.GetStale(ctx, []byte(args[1]))
		} else {
			value, found, err = client.Get(ctx, []byte(args[1]))
		}
		if err != nil {
			return describe(err)
		}
		if !found {
			return fmt.Errorf("key %q not found", args[1])
		}
		fmt.Println(string(value))

	case "delete":
		if len(args) != 2 {
			return errors.New("usage: kvctl delete <key>")
		}
		existed, err := client.Delete(ctx, []byte(args[1]))
		if err != nil {
			return describe(err)
		}
		if existed {
			fmt.Println("deleted")
		} else {
			fmt.Println("no such key")
		}

	case "status":
		return printStatus(ctx, client)

	case "members":
		if len(args) < 2 {
			return errors.New("usage: kvctl members <id> [<id>...]")
		}
		voters, err := parseIDs(args[1:])
		if err != nil {
			return err
		}
		resp, err := client.ChangeMembership(ctx, voters)
		if err != nil {
			return describe(err)
		}
		fmt.Printf("cluster is now %v (committed at index %d)\n",
			resp.GetConfig().GetVoters(), resp.GetIndex())

	default:
		usage()
		return fmt.Errorf("unknown command %q", args[0])
	}
	return nil
}

// statusTimeout bounds each node's status call, so one unreachable node cannot
// consume the whole command timeout and leave the rest unqueried.
const statusTimeout = 2 * time.Second

// membershipTimeout is the floor for a membership change, which is two commits
// and a round of catching the new member up rather than a single write.
const membershipTimeout = 30 * time.Second

// printStatus asks every endpoint what it believes, rather than following the
// leader. Disagreement between nodes is the interesting part — a node that
// thinks someone else leads, or that is behind on commit index, is exactly what
// an operator is looking for.
func printStatus(ctx context.Context, client *kvserver.Client) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	// Writes to a tabwriter are buffered and cannot fail; Flush is the call
	// that reports an error, and it is checked below.
	_, _ = fmt.Fprintln(w, "ENDPOINT\tNODE\tSTATE\tTERM\tLEADER\tCOMMIT\tAPPLIED\tLAG")

	reachable := 0
	for _, endpoint := range client.Endpoints() {
		each, cancel := context.WithTimeout(ctx, statusTimeout)
		status, err := client.Status(each, endpoint)
		cancel()
		if err != nil {
			_, _ = fmt.Fprintf(w, "%s\tunreachable\t\t\t\t\t\t\n", endpoint)
			continue
		}
		reachable++

		lag := "-"
		if len(status.GetMatchIndex()) > 0 {
			worst := uint64(0)
			for _, match := range status.GetMatchIndex() {
				if behind := status.GetCommitIndex() - min(match, status.GetCommitIndex()); behind > worst {
					worst = behind
				}
			}
			lag = fmt.Sprintf("%d", worst)
		}

		_, _ = fmt.Fprintf(w, "%s\t%d\t%s\t%d\t%d\t%d\t%d\t%s\n",
			endpoint, status.GetNodeId(), status.GetState(), status.GetTerm(),
			status.GetLeaderId(), status.GetCommitIndex(), status.GetAppliedIndex(), lag)
	}
	if err := w.Flush(); err != nil {
		return err
	}

	total := len(client.Endpoints())
	if quorum := total/2 + 1; reachable < quorum {
		return fmt.Errorf("only %d of %d nodes reachable, below the quorum of %d: "+
			"the cluster cannot commit anything", reachable, total, quorum)
	}
	return nil
}

// describe adds the context a bare gRPC status does not carry.
func describe(err error) error {
	if hint, ok := kvserver.LeaderHint(err); ok && hint.GetLeaderHint() != 0 {
		return fmt.Errorf("%w (the leader is node %d at %s)",
			err, hint.GetLeaderHint(), hint.GetLeaderAddress())
	}
	return err
}

func parseIDs(args []string) ([]uint64, error) {
	out := make([]uint64, 0, len(args))
	for _, a := range args {
		id, err := strconv.ParseUint(strings.TrimSpace(a), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%q is not a node id: %w", a, err)
		}
		if id == 0 {
			return nil, fmt.Errorf("node id 0 is reserved to mean 'no node'")
		}
		out = append(out, id)
	}
	return out, nil
}

func splitEndpoints(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func usage() {
	fmt.Fprint(os.Stderr, `kvctl — a client for a raftkv cluster

Usage:
  kvctl [flags] set <key> <value>
  kvctl [flags] get <key>
  kvctl [flags] delete <key>
  kvctl [flags] status
  kvctl [flags] members <id> [<id>...]

Reads are linearizable by default: the node confirms with a quorum that it is
still the leader before answering, so a partitioned leader returns an error
rather than a stale value. Pass --stale to skip that.

"members" lists the complete new voter set, not a delta. The cluster moves there
through a joint configuration that needs majorities of both the old and new
membership, so the two can never elect separate leaders mid-change. The command
returns once the whole transition has finished. A node must already be running
and reachable before it can be added: joining changes who is counted, not who
exists.

Flags:
`)
	flag.PrintDefaults()
}

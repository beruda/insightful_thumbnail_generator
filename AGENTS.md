# AGENTS.md

Guidance for AI coding agents working in this repository. Read `README.md` first for the design; this file covers
how to work on the code without breaking it.

## Setup

- Go 1.25+ (uses `testing/synctest` and `sync.WaitGroup.Go`). Homebrew installs it to `/opt/homebrew/bin`; add that to
  `PATH` if `go` isn't found.
- ffmpeg is optional. Without it, the smoke tests in `internal/adapters` skip themselves.
- No other services are needed: the queue, stores and file systems used by tests are all in memory.

## Commands

```sh
make test            # full suite
make race            # full suite under the race detector (run this before committing)
make test-short      # fakes only, no ffmpeg subprocesses
make lint            # gofmt -l + go vet
make cover           # statement coverage across internal/
make chaos SEED=3    # rerun one chaos-test seed verbosely
make load FILES=200000 ARGS="-list-latency 20ms -open-latency 5ms"   # synthetic load test
make demo ROOT=<dir> # real run over a directory with real ffmpeg
```

Done means: `gofmt -l .` prints nothing, `go vet ./...` is clean, and `go test -race ./...` passes.

## Layout

| Package | Role |
|---|---|
| `cmd/thumbnailer` | Service binary: flags, real adapters, signals, `/healthz` |
| `cmd/loadrun` | Load-test binary |
| `internal/domain` | Shared types and interfaces. Depends on no other internal package |
| `internal/scanner` | Queue-driven scan: one directory page per `DirJob` |
| `internal/queue` | `Queue` interface + in-memory implementation |
| `internal/worker` | One job end to end: ffmpeg supervision, failure classification, commit, events |
| `internal/storage` | Blob key scheme, in-memory stores |
| `internal/adapters` | OS file system, ffmpeg, scratch dir, local blob store, SQLite |
| `internal/app` | Config/flags, roles, service lifecycle, health handler |
| `internal/metrics` | In-process counters |
| `internal/testkit` | Test-only fakes with fault injection, the harness, assert helpers |
| `internal/loadtest` | Generated file system, latency wrapper, in-process fake ffmpeg, load report |

## Invariants: don't break these

Each one has tests; if a change makes one fail, the change is wrong, not the test.

- **Delivery is at-least-once, so the runner must stay idempotent.** A settled job is acked without running ffmpeg;
  blob keys are deterministic per (path, version); the `Notified` flag lets a retry finish sending an event.
- **Commit order is blob → record → event → ack.** Never ack before the record is written.
- **A ready thumbnail is never downgraded** to failed or dead-lettered. Failures after commit go through
  `notifyFailed`, never `fail`.
- **The source is re-checked before and after ffmpeg.** If it changed or vanished, the job is superseded and nothing
  is written.
- **The scan queue stays unbounded.** Scanners publish to the queue they consume; a capacity limit there can
  deadlock them. Backpressure belongs on the video queue.
- **The scanner's outstanding counter is incremented before a `DirJob` is published** and decremented only when a
  job is acked or dead-lettered. Otherwise a pass can end early.
- **In `queue.Memory`, every state change and its notification happen under the same lock.** Notifying before
  freeing a slot causes a lost wake-up that tests only catch intermittently.
- **Retries go through the queue** (`Nack` with a backoff delay), never by sleeping in a worker.
- **Every external call takes a `context.Context`** and honors cancellation.

## Testing conventions

- Tests are external packages (`package foo_test`) and build systems from `internal/testkit`:
  `tk.NewHarness(opts...)` wires everything from fakes; `h.Runner()`, `h.Scanner()`, `h.Service()` give the piece
  under test; `h.Restart()` simulates a process restart.
- Anything involving time (timeouts, backoff, latency, shutdown grace) runs inside
  `synctest.Test(t, func(t *testing.T) { ... })`. Create the harness *inside* the bubble. Time asserts can be exact.
  A test that leaves an unkillable fake process behind must call `h.Launcher.ForceExitAll()` before returning, or
  the bubble reports a deadlock.
- Inject faults with the testkit, not by hand-rolled mocks: `h.FS.FailList/FailListTimes/FailOpen/WithLatency`,
  `h.Repo.FailNext/FailAlways/FailRandomly`, `h.Launcher.Script(path, tk.Hang(0), tk.Succeed())`, and so on.
- Assert with `tk.Equal` / `tk.True`; there's no third-party assertion library.
- Each pipeline stage has its own suite: walk and read (`scanner_test.go`), process (`runner_process_test.go`),
  write and done (`runner_commit_test.go`). Put new cases in the matching one.
- Behavior changes to scanning or the worker should keep `internal/app/chaos_test.go` passing on all seeds; if an
  invariant there needs to change, say why in the commit message.
- A regression found in review gets a test that fails without the fix. Check that it does.
- `testdata/rapid/` holds rapid's failure records from failing property runs. It's gitignored; don't commit it.

## Code style

- Match the surrounding code: comments explain *why* (tradeoffs, failure modes), not what the next line does.
- Interfaces live in `domain` (or `queue` for the queue); adapters implement them; nothing outside `cmd/` and tests
  constructs real adapters.
- Keep error kinds in `worker/failures.go`. A new permanent ffmpeg error is a new pattern there plus a row in
  `TestClassifyProcessFailure`, and ideally a real-ffmpeg sample in the smoke tests.
- Config flags go through `app.ParseFlags`, with validation and a test in `config_test.go`.
- Update `README.md` when behavior, numbers or known limits change.

## Git

- Work on `main` only when asked; otherwise use a branch. Don't rewrite or reset existing history.
- `instructions.pdf` (the assignment brief) stays untracked.
- End commit messages with the co-author line the session specifies.

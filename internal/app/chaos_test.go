package app_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/vukkremic/thumbnailer/internal/app"
	"github.com/vukkremic/thumbnailer/internal/domain"
	"github.com/vukkremic/thumbnailer/internal/queue"
	tk "github.com/vukkremic/thumbnailer/internal/testkit"
)

type queueStats = queue.Stats

const (
	chaosFiles       = 1000
	chaosConcurrency = 8
	chaosMaxInput    = 900_000
)

// TestChaos runs about 1k videos with every failure mode at once: process failures, hangs, bad
// output, oversized inputs, and flaky storage and event bus. It then checks the invariants that
// must hold no matter what. Reproduce a failure with: CHAOS_SEED=<seed> go test ./internal/app -run Chaos
func TestChaos(t *testing.T) {
	seeds := []uint64{1, 2, 3, 4, 5}
	if s := os.Getenv("CHAOS_SEED"); s != "" {
		seed, err := strconv.ParseUint(s, 10, 64)
		tk.Equal(t, err, nil, "CHAOS_SEED")
		seeds = []uint64{seed}
	}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) { chaos(t, seed) })
		})
	}
}

func chaos(t *testing.T, seed uint64) {
	random := tk.Seeded(seed)
	pick := func(n int) int { return int(random() * float64(n)) }
	transient := func() tk.Script {
		return []tk.Script{
			tk.ExitWith(1, tk.TransientStderr), tk.Hang(0), tk.Hang(1), tk.SpawnError(),
			tk.SucceedWithOutput(nil), tk.SucceedWithOutput([]byte{1, 2}),
		}[pick(6)]
	}

	h := tk.NewHarness(tk.WithSeed(seed), tk.Configure(func(c *app.Config) {
		c.Worker.Concurrency = chaosConcurrency
		c.Runner.MaxAttempts = 4
		c.Runner.ProcessTimeout = 5 * time.Second
		c.Runner.MaxInputBytes = chaosMaxInput
	}))

	var videos []string
	for i := range chaosFiles {
		dir := fmt.Sprintf("/videos/d%d/e%d", pick(20), pick(5))
		isVideo := random() < 0.9
		ext := []string{"mp4", "MOV", "mkv"}[pick(3)]
		if !isVideo {
			ext = "txt"
		}
		path := fmt.Sprintf("%s/f%d.%s", dir, i, ext)
		size := int64(1 + pick(10_000))
		if random() < 0.03 {
			size = 1_000_000 // over the input limit
		}
		h.FS.File(path, tk.Size(size), tk.ModTimeMs(int64(i)))
		if !isVideo {
			continue
		}
		videos = append(videos, path)

		switch roll := random(); {
		case roll < 0.60:
			h.Launcher.Script(path, tk.SucceedAfter(time.Duration(10+pick(2000))*time.Millisecond))
		case roll < 0.75:
			h.Launcher.Script(path, transient(), tk.Succeed())
		case roll < 0.85:
			h.Launcher.Script(path, transient(), transient(), transient(), tk.Succeed())
		case roll < 0.92:
			h.Launcher.Script(path, transient()) // never recovers: exhausts its attempts
		default:
			h.Launcher.Script(path, tk.ExitWith(1, tk.CorruptStderr)) // permanent
		}
	}
	h.Blobs.FailRandomly("Put", 0.05, random)
	h.Repo.FailRandomly("Upsert", 0.05, random)
	h.Events.FailRandomly("Publish", 0.05, random)

	fail := func(format string, args ...any) {
		t.Helper()
		t.Fatalf("seed %d (rerun: CHAOS_SEED=%d go test ./internal/app -run Chaos): %s", seed, seed, fmt.Sprintf(format, args...))
	}

	if err := h.Service().Run(context.Background()); err != nil {
		fail("Run: %v", err)
	}

	// 1. Exactly one terminal state per video: ready or dead-lettered. Never both, never neither.
	records := h.Repo.All()
	ready := map[string]bool{}
	for p, r := range records {
		if r.Status == domain.StatusReady {
			ready[p] = true
		}
	}
	dead := map[string]queue.DeadLetter[domain.VideoJob]{}
	for _, d := range h.Queue.DeadLetters() {
		if _, dup := dead[d.Message.Path]; dup {
			fail("%s dead-lettered twice", d.Message.Path)
		}
		if ready[d.Message.Path] {
			fail("%s is both ready and dead-lettered (%s)", d.Message.Path, d.Reason)
		}
		dead[d.Message.Path] = d
	}
	for _, v := range videos {
		if !ready[v] && dead[v].Reason == "" {
			fail("%s has no terminal state", v)
		}
	}
	if len(ready)+len(dead) != len(videos) {
		fail("%d ready + %d dead != %d videos", len(ready), len(dead), len(videos))
	}

	// 2. Every ready record points at a stored blob. Its event was sent, or it's flagged for the
	//    next scan to send (Notified=false).
	blobs := h.Blobs.Objects()
	announced := map[string]bool{}
	for _, e := range h.Events.Published() {
		announced[e.VideoPath] = true
	}
	for p := range ready {
		rec := records[p]
		if _, ok := blobs[rec.BlobKey]; !ok {
			fail("ready record %s points at missing blob", p)
		}
		if !announced[p] && rec.Notified {
			fail("%s marked notified but no event was published", p)
		}
	}

	// 3. Oversized and corrupt inputs were never retried; oversized ones never even spawned ffmpeg.
	for p, d := range dead {
		if strings.HasPrefix(d.Reason, "input_too_large") || strings.HasPrefix(d.Reason, "corrupt_input") {
			if d.DeliveryCount != 1 {
				fail("%s (%s) was retried %d times", p, d.Reason, d.DeliveryCount)
			}
		}
		if strings.HasPrefix(d.Reason, "input_too_large") && h.Launcher.SpawnsFor(p) != 0 {
			fail("ffmpeg ran for oversized input %s", p)
		}
	}

	// 4. Resources: the concurrency cap held, no process leaked, no scratch file left, queue empty.
	if peak := h.Launcher.PeakRunning(); peak > chaosConcurrency {
		fail("peak concurrency %d > %d", peak, chaosConcurrency)
	}
	if leaked := h.Launcher.Leaked(); len(leaked) > 0 {
		fail("leaked processes: %v", leaked)
	}
	if n := len(h.Scratch.Files()); n > 0 {
		fail("%d scratch files left", n)
	}
	if s := h.Queue.Stats(); s.Ready+s.Delayed+s.InFlight != 0 {
		fail("queue not drained: %+v", s)
	}

	// 5. The counters agree with the state.
	if g := h.Metrics.Get("worker.generated"); g != int64(len(ready)) {
		fail("generated counter %d != %d ready", g, len(ready))
	}
	if d := h.Metrics.Get("worker.dead_lettered"); d != int64(len(dead)) {
		fail("dead_lettered counter %d != %d dead letters", d, len(dead))
	}

	// 6. One more scan pass (with healthy dependencies) delivers every deferred event. The only
	//    videos it may run ffmpeg for again are dead-lettered ones whose failure record couldn't be
	//    written: without a record, retrying them on the next pass is the intended behaviour.
	unnotified := 0
	for p := range ready {
		if !records[p].Notified {
			unnotified++
		}
	}
	unrecorded := map[string]bool{}
	for p := range dead {
		if _, ok := records[p]; !ok {
			unrecorded[p] = true
		}
	}
	h.Blobs.Heal()
	h.Repo.Heal()
	h.Events.Heal()
	spawnsBefore := map[string]int{}
	for _, v := range videos {
		spawnsBefore[v] = h.Launcher.SpawnsFor(v)
	}
	if err := h.Restart().Service.Run(context.Background()); err != nil {
		fail("sweep run: %v", err)
	}
	for p := range ready {
		if !h.Repo.All()[p].Notified {
			fail("%s still not notified after the sweep", p)
		}
	}
	for _, v := range videos {
		if h.Launcher.SpawnsFor(v) != spawnsBefore[v] && !unrecorded[v] {
			fail("the sweep re-ran ffmpeg for %s", v)
		}
	}
	t.Logf("seed %d: %d videos, %d ready, %d dead-lettered, %d events deferred to the sweep",
		seed, len(videos), len(ready), len(dead), unnotified)
}

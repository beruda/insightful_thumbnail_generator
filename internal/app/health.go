package app

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/vukkremic/thumbnailer/internal/domain"
)

// HealthHandler serves GET /healthz: a liveness probe for orchestration (k8s) that also returns
// status JSON (counters, queue depth) for quick inspection.
func HealthHandler(status func() map[string]any) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		body := map[string]any{"status": "ok"}
		for k, v := range status() {
			body[k] = v
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	})
	return mux
}

// LogEvents publishes "thumbnail ready" events as log lines. Production would use a Pub/Sub topic or SNS.
type LogEvents struct{ Logger *slog.Logger }

func (e LogEvents) Publish(_ context.Context, ev domain.ReadyEvent) error {
	e.Logger.Info("event", "type", ev.Type, "videoPath", ev.VideoPath, "blobKey", ev.BlobKey)
	return nil
}

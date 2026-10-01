package app_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/vukkremic/thumbnailer/internal/app"
	tk "github.com/vukkremic/thumbnailer/internal/testkit"
)

func TestParseFlags(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		o, err := app.ParseFlags([]string{"-root", "/videos"}, io.Discard)
		tk.Equal(t, err, nil, "error")
		tk.Equal(t, [4]any{o.Service.Role, o.Service.Once, o.Service.Runner.ProcessTimeout, o.Service.Runner.MaxAttempts},
			[4]any{app.RoleAll, false, 2 * time.Minute, 4}, "defaults")
		tk.True(t, o.Service.Worker.Concurrency >= 1, "concurrency")
	})

	t.Run("overrides", func(t *testing.T) {
		o, err := app.ParseFlags([]string{"-root", "/v", "-once", "-concurrency", "3", "-timeout", "30s", "-max-input-mb", "2"}, io.Discard)
		tk.Equal(t, err, nil, "error")
		tk.Equal(t, [4]any{o.Service.Once, o.Service.Worker.Concurrency, o.Service.Runner.ProcessTimeout, o.Service.Runner.MaxInputBytes},
			[4]any{true, 3, 30 * time.Second, int64(2 << 20)}, "overrides")
	})

	for name, args := range map[string][]string{
		"missing root":     {},
		"bad role":         {"-root", "/v", "-role", "boss"},
		"zero concurrency": {"-root", "/v", "-concurrency", "0"},
		"negative timeout": {"-root", "/v", "-timeout", "-1s"},
		"bad log level":    {"-root", "/v", "-log-level", "loud"},
		"unknown flag":     {"-root", "/v", "-nope"},
	} {
		t.Run("rejects "+name, func(t *testing.T) {
			_, err := app.ParseFlags(args, io.Discard)
			tk.True(t, err != nil, "expected an error for %v", args)
		})
	}

	t.Run("role=worker doesn't need a root", func(t *testing.T) {
		_, err := app.ParseFlags([]string{"-role", "worker"}, io.Discard)
		tk.Equal(t, err, nil, "error")
	})
}

func TestHealthHandler(t *testing.T) {
	srv := httptest.NewServer(app.HealthHandler(func() map[string]any { return map[string]any{"queued": 3} }))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	tk.Equal(t, err, nil, "GET")
	defer resp.Body.Close()
	var body map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&body)
	tk.Equal(t, [3]any{resp.StatusCode, body["status"], body["queued"]}, [3]any{200, "ok", float64(3)}, "response")

	resp2, _ := http.Post(srv.URL+"/healthz", "text/plain", strings.NewReader(""))
	tk.Equal(t, resp2.StatusCode, http.StatusMethodNotAllowed, "POST")
	resp3, _ := http.Get(srv.URL + "/other")
	tk.Equal(t, resp3.StatusCode, http.StatusNotFound, "unknown path")
}

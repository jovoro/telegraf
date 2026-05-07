package slurm

import (
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/serializers/influx"
	"github.com/influxdata/telegraf/testutil"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "update expected.out files from actual plugin output")

// TestGenerateExpected regenerates expected.out for all testcases.
// Run with: go test ./plugins/inputs/slurm/ -run TestGenerateExpected -update
func TestGenerateExpected(t *testing.T) {
	if !*update {
		t.Skip("skipping; run with -update to regenerate expected.out files")
	}

	entries, err := os.ReadDir("testcases")
	require.NoError(t, err)

	serializer := &influx.Serializer{}
	require.NoError(t, serializer.Init())

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		t.Run(entry.Name(), func(t *testing.T) {
			testcasePath := filepath.Join("testcases", entry.Name())
			responsesPath := filepath.Join(testcasePath, "responses")
			configFilename := filepath.Join(testcasePath, "telegraf.conf")
			expectedFilename := filepath.Join(testcasePath, "expected.out")

			responses, err := os.ReadDir(responsesPath)
			require.NoError(t, err)

			pathToResponse := map[string][]byte{}
			for _, response := range responses {
				if response.IsDir() {
					continue
				}
				fName := response.Name()
				buf, err := os.ReadFile(filepath.Join(responsesPath, fName))
				require.NoError(t, err)
				pathToResponse[strings.TrimSuffix(fName, filepath.Ext(fName))] = buf
			}

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				resp, ok := pathToResponse[filepath.Base(r.URL.Path)]
				if !ok {
					w.WriteHeader(http.StatusInternalServerError)
					t.Errorf("no fixture for path: %s", r.URL.Path)
					return
				}
				w.Header().Add("Content-Type", "application/json")
				if _, err := w.Write(resp); err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					t.Error(err)
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer ts.Close()

			cfg := config.NewConfig()
			cfg.Agent.Quiet = true
			require.NoError(t, cfg.LoadConfig(configFilename))
			require.Len(t, cfg.Inputs, 1)

			plugin := cfg.Inputs[0].Input.(*Slurm)
			plugin.URL = "http://" + ts.Listener.Addr().String()
			plugin.Log = testutil.Logger{}
			require.NoError(t, plugin.Init())

			var acc testutil.Accumulator
			require.NoError(t, plugin.Gather(&acc))

			f, err := os.Create(expectedFilename)
			require.NoError(t, err)
			defer f.Close()

			for _, m := range acc.GetTelegrafMetrics() {
				b, err := serializer.Serialize(m)
				require.NoError(t, err)
				f.Write(b)
			}
			t.Logf("wrote %d metrics to %s", len(acc.GetTelegrafMetrics()), expectedFilename)
		})
	}
}

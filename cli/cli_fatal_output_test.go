package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type failingStdoutWriter struct{ err error }

func (f *failingStdoutWriter) Write(p []byte) (int, error) { return 0, f.err }

// a stdout write failure must surface as a command error (non-zero process
// exit) and stop further publishing, rather than being silently swallowed
func TestFailingStdoutReturnsCommandError(t *testing.T) {
	t.Parallel()

	server := NewHttpServerStub(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"ok":true}`)
	}))
	defer server.Close()

	boom := errors.New("stdout exploded")
	stderr := new(strings.Builder)
	command := SetupCommand(testBuildInfo, server.stubStdinUrls([]string{"a", "b", "c", "d"}), stderr, &failingStdoutWriter{err: boom})

	err := command.Run(context.Background(), []string{"ganda", "-W", "4", "-J"})

	assert.Error(t, err)
	assert.ErrorIs(t, err, boom)
	// the failure is explicitly reported on stderr (non-silent run)
	assert.Contains(t, stderr.String(), "stdout exploded")
}

// in silent mode stderr stays empty but the command still returns the fatal
// error so the process exit code remains non-zero
func TestFailingStdoutSilentStillReturnsError(t *testing.T) {
	t.Parallel()

	server := NewHttpServerStub(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"ok":true}`)
	}))
	defer server.Close()

	boom := errors.New("stdout exploded")
	stderr := new(strings.Builder)
	command := SetupCommand(testBuildInfo, server.stubStdinUrls([]string{"a", "b"}), stderr, &failingStdoutWriter{err: boom})

	err := command.Run(context.Background(), []string{"ganda", "-W", "2", "-J", "-s"})

	assert.Error(t, err)
	assert.ErrorIs(t, err, boom)
	assert.Equal(t, "", stderr.String(), "silent mode must keep stderr empty")
}

var _ io.Writer = (*failingStdoutWriter)(nil)

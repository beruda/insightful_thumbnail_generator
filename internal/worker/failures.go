package worker

import (
	"fmt"
	"regexp"
	"strings"
)

type FailureKind string

const (
	KindTimeout          FailureKind = "timeout"           // ffmpeg exceeded the time budget and was killed
	KindProcessError     FailureKind = "process_error"     // non-zero exit we can't attribute to the input
	KindSpawnError       FailureKind = "spawn_error"       // the process couldn't be started
	KindCorruptInput     FailureKind = "corrupt_input"     // ffmpeg can't decode the file
	KindUnsupportedCodec FailureKind = "unsupported_codec" //
	KindNoVideoStream    FailureKind = "no_video_stream"   // e.g. an audio-only file with a video extension
	KindInputUnavailable FailureKind = "input_unavailable" // missing / unreadable source
	KindInputTooLarge    FailureKind = "input_too_large"   //
	KindInvalidOutput    FailureKind = "invalid_output"    // exit 0, but output is missing, empty, or not an image
	KindOutputTooLarge   FailureKind = "output_too_large"  //
	KindStorageError     FailureKind = "storage_error"     // blob store / repository failure
)

// Failure describes why an attempt failed and whether trying again could help.
type Failure struct {
	Kind FailureKind
	// Retryable failures are retried with backoff; permanent ones go straight to the dead-letter queue.
	Retryable bool
	Message   string
}

func (f Failure) Error() string { return fmt.Sprintf("%s: %s", f.Kind, f.Message) }

// permanentPatterns are stderr messages meaning "this input will never work", so retrying only
// wastes worker time. Checked against real ffmpeg output in the smoke tests.
var permanentPatterns = []struct {
	re   *regexp.Regexp
	kind FailureKind
}{
	{regexp.MustCompile(`(?i)Invalid data found when processing input`), KindCorruptInput},
	{regexp.MustCompile(`(?i)moov atom not found`), KindCorruptInput},
	{regexp.MustCompile(`(?i)could not find codec parameters`), KindCorruptInput},
	{regexp.MustCompile(`(?i)Output file is empty, nothing was encoded`), KindCorruptInput},
	{regexp.MustCompile(`(?i)does not contain any stream|matches no streams`), KindNoVideoStream},
	{regexp.MustCompile(`(?i)Decoder \(codec [^)]*\) not found|Unsupported codec|unknown codec`), KindUnsupportedCodec},
	{regexp.MustCompile(`(?i)No such file or directory|Permission denied`), KindInputUnavailable},
}

const maxMessageLength = 2000

// ClassifyProcessFailure maps a non-zero ffmpeg exit to a Failure.
func ClassifyProcessFailure(exitCode int, stderr string) Failure {
	message := SummarizeStderr(stderr)
	if message == "" {
		message = fmt.Sprintf("ffmpeg exited with code %d", exitCode)
	}
	for _, p := range permanentPatterns {
		if p.re.MatchString(stderr) {
			return Failure{Kind: p.kind, Retryable: false, Message: message}
		}
	}
	// Anything else (OOM kill, I/O hiccup on a remote mount, crash) is assumed to be transient.
	return Failure{Kind: KindProcessError, Retryable: true, Message: message}
}

// SummarizeStderr keeps the tail of stderr, where ffmpeg puts the actual error; the head is noise.
func SummarizeStderr(stderr string) string {
	s := strings.TrimSpace(stderr)
	if len(s) <= maxMessageLength {
		return s
	}
	return "…" + s[len(s)-maxMessageLength:]
}

func storageFailure(operation string, err error) Failure {
	return Failure{Kind: KindStorageError, Retryable: true, Message: fmt.Sprintf("%s failed: %v", operation, err)}
}

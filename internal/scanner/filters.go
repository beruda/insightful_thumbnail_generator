package scanner

import (
	"path"
	"strings"

	"github.com/vukkremic/thumbnailer/internal/domain"
)

var DefaultVideoExtensions = []string{
	"mp4", "m4v", "mov", "mkv", "webm", "avi", "wmv", "flv", "mpg", "mpeg", "3gp", "mts", "m2ts",
}

type SkipReason string

const (
	Accepted       SkipReason = ""
	SkipNotVideo   SkipReason = "not_video"
	SkipEmpty      SkipReason = "empty"
	SkipUnreadable SkipReason = "unreadable"
)

const readBits = 0o444

// ExtensionSet normalizes extensions ("MP4", ".mp4" → "mp4") into a lookup set.
func ExtensionSet(extensions []string) map[string]bool {
	set := make(map[string]bool, len(extensions))
	for _, ext := range extensions {
		set[strings.ToLower(strings.TrimPrefix(ext, "."))] = true
	}
	return set
}

// Classify decides whether a file is a video we should make a thumbnail for. It returns
// Accepted, or the reason the file is skipped. A pure function, so it's trivial to test.
func Classify(f domain.FileInfo, extensions map[string]bool) SkipReason {
	if f.IsDir || !extensions[extensionOf(f.Name)] {
		return SkipNotVideo
	}
	if f.Size <= 0 {
		return SkipEmpty
	}
	if f.Permissions&readBits == 0 {
		return SkipUnreadable
	}
	return Accepted
}

func extensionOf(name string) string {
	if strings.HasPrefix(name, ".") && strings.Count(name, ".") == 1 {
		return "" // a dotfile like ".mp4" has no extension
	}
	return strings.ToLower(strings.TrimPrefix(path.Ext(name), "."))
}

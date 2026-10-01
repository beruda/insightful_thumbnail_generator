package scanner_test

import (
	"testing"

	"github.com/vukkremic/thumbnailer/internal/domain"
	"github.com/vukkremic/thumbnailer/internal/scanner"
	tk "github.com/vukkremic/thumbnailer/internal/testkit"
)

func info(name string, edit ...func(*domain.FileInfo)) domain.FileInfo {
	f := domain.FileInfo{Path: "/v/" + name, Name: name, Permissions: 0o644, Size: 100}
	for _, e := range edit {
		e(&f)
	}
	return f
}

func TestClassify(t *testing.T) {
	exts := scanner.ExtensionSet(scanner.DefaultVideoExtensions)

	for _, name := range []string{"a.mp4", "b.MOV", "c.Mkv", "d.webm", "e.m2ts", "archive.2024.avi"} {
		tk.Equal(t, scanner.Classify(info(name), exts), scanner.Accepted, name)
	}
	for _, name := range []string{"notes.txt", "main.go", "mp4", "video.mp4.part", "noext", ".mp4"} {
		tk.Equal(t, scanner.Classify(info(name), exts), scanner.SkipNotVideo, name)
	}

	cases := []struct {
		name string
		file domain.FileInfo
		want scanner.SkipReason
	}{
		{"directory named like a video", info("folder.mp4", func(f *domain.FileInfo) { f.IsDir = true }), scanner.SkipNotVideo},
		{"zero bytes", info("a.mp4", func(f *domain.FileInfo) { f.Size = 0 }), scanner.SkipEmpty},
		{"no read permission", info("a.mp4", func(f *domain.FileInfo) { f.Permissions = 0o200 }), scanner.SkipUnreadable},
		{"readable by owner only", info("a.mp4", func(f *domain.FileInfo) { f.Permissions = 0o400 }), scanner.Accepted},
		{"readable by group only", info("a.mp4", func(f *domain.FileInfo) { f.Permissions = 0o040 }), scanner.Accepted},
		{"readable by others only", info("a.mp4", func(f *domain.FileInfo) { f.Permissions = 0o004 }), scanner.Accepted},
	}
	for _, c := range cases {
		tk.Equal(t, scanner.Classify(c.file, exts), c.want, c.name)
	}

	tk.Equal(t, scanner.Classify(info("a.xyz"), scanner.ExtensionSet([]string{".XYZ"})), scanner.Accepted, "normalized extension")
}

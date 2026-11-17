package tempfiles

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAbandonedFilesAndLiveOwner(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"download-old.tmp", "field-old.tmp", "http-old.tmp", "keep.tmp"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("data"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	owner, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	for _, name := range []string{"download-old.tmp", "field-old.tmp", "http-old.tmp"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("orphan remains: %s: %v", name, err)
		}
	}
	live := filepath.Join(dir, "download-live.tmp")
	if err := os.WriteFile(live, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if other, err := Acquire(dir); err == nil {
		other.Close()
		t.Fatal("second owner accepted")
	}
	for _, path := range []string{live, filepath.Join(dir, "keep.tmp")} {
		if _, err := os.Stat(path); err != nil {
			t.Fatal(err)
		}
	}
	owner.Close()
	next, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	next.Close()
	if _, err := os.Stat(live); !os.IsNotExist(err) {
		t.Fatalf("abandoned live file remains: %v", err)
	}
}

package namespace

import (
	"testing"

	"github.com/warpdrive/warpdrive/pkg/config"
)

func TestResolve(t *testing.T) {
	ns := New([]config.BackendConfig{
		{Name: "azure_training", MountPath: "/training_sets"},
		{Name: "s3_datasets", MountPath: "/datasets"},
	})

	tests := []struct {
		path        string
		wantBackend string
		wantRemote  string
		wantErr     bool
	}{
		{"/training_sets/imagenet/shard-0000.tar", "azure_training", "imagenet/shard-0000.tar", false},
		{"/training_sets", "azure_training", "", false},
		{"training_sets/file.txt", "azure_training", "file.txt", false},
		{"/datasets/mydata.csv", "s3_datasets", "mydata.csv", false},
		{"/unknown/path", "", "", true},
	}

	for _, tt := range tests {
		res, err := ns.Resolve(tt.path)
		if tt.wantErr {
			if err == nil {
				t.Errorf("Resolve(%q) expected error", tt.path)
			}
			continue
		}
		if err != nil {
			t.Errorf("Resolve(%q) error: %v", tt.path, err)
			continue
		}
		if res.BackendName != tt.wantBackend {
			t.Errorf("Resolve(%q) backend = %q, want %q", tt.path, res.BackendName, tt.wantBackend)
		}
		if res.RemotePath != tt.wantRemote {
			t.Errorf("Resolve(%q) remote = %q, want %q", tt.path, res.RemotePath, tt.wantRemote)
		}
	}
}

func TestMountPoints(t *testing.T) {
	ns := New([]config.BackendConfig{
		{Name: "a", MountPath: "/training_sets"},
		{Name: "b", MountPath: "/datasets"},
		{Name: "c", MountPath: "/archive"},
	})

	pts := ns.MountPoints()
	if len(pts) != 3 {
		t.Fatalf("MountPoints len = %d, want 3", len(pts))
	}
}

func TestIsRootPath(t *testing.T) {
	ns := New([]config.BackendConfig{
		{Name: "a", MountPath: "/training_sets"},
	})

	if !ns.IsRootPath("/") {
		t.Error("IsRootPath(/) should be true")
	}
	if !ns.IsRootPath("") {
		t.Error("IsRootPath('') should be true")
	}
	if ns.IsRootPath("/training_sets") {
		t.Error("IsRootPath(/training_sets) should be false")
	}
}

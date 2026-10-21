package recconf

import (
	"os"
	"path/filepath"
	"testing"
)

func TestExpandAccessKeyPlaceholders(t *testing.T) {
	t.Setenv("AccessKey", "test-id")
	t.Setenv("AccessSecret", "test-secret")
	out, err := expandAccessKeyPlaceholders([]byte(`{"a":"${AccessKey}","b":"${AccessSecret}","c":"${bizdate}"}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != `{"a":"test-id","b":"test-secret","c":"${bizdate}"}` {
		t.Fatalf("got %s", out)
	}

	t.Setenv("AccessSecret", "")
	if _, err := expandAccessKeyPlaceholders([]byte(`{"b":"${AccessSecret}"}`)); err == nil {
		t.Fatal("placeholder without env should be an error")
	}
	if out, err := expandAccessKeyPlaceholders([]byte(`{"b":"plain"}`)); err != nil || string(out) != `{"b":"plain"}` {
		t.Fatalf("no placeholder: out=%s err=%v", out, err)
	}
}

func TestLoadConfigExpandsAccessKeyPlaceholders(t *testing.T) {
	t.Setenv("AccessKey", "test-id")
	t.Setenv("AccessSecret", "test-secret")
	path := filepath.Join(t.TempDir(), "config.json")
	body := `{"FeatureStoreConfs":{"fs":{"AccessId":"${AccessKey}","AccessKey":"${AccessSecret}","RegionId":"cn-beijing"}}}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := LoadConfig(path); err != nil {
		t.Fatal(err)
	}
	fs := Config.FeatureStoreConfs["fs"]
	if fs.AccessId != "test-id" || fs.AccessKey != "test-secret" {
		t.Fatalf("got %+v", fs)
	}
}

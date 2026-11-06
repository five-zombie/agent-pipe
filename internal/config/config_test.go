package config

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestInitSkipsExistingConfigAndAddPipeDoesNotDuplicate(t *testing.T) {
	path := filepath.Join(t.TempDir(), ConfigFileName)

	cfg, created, err := Init(path)
	if err != nil {
		t.Fatal(err)
	}
	if !created || cfg.ClientID == "" {
		t.Fatalf("expected new config with client id, created=%v client_id=%q", created, cfg.ClientID)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	loaded, created, err := Init(path)
	if err != nil {
		t.Fatal(err)
	}
	if created || loaded.ClientID != cfg.ClientID {
		t.Fatalf("expected unchanged existing config, created=%v", created)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("pipecli init changed an existing config")
	}

	registered := Pipe{ID: "pipe-1", Name: "work", Desc: "work pipe"}
	added, err := AddPipe(path, loaded, registered)
	if err != nil || !added {
		t.Fatalf("add pipe: added=%v err=%v", added, err)
	}
	loaded, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	added, err = AddPipe(path, loaded, registered)
	if err != nil || added {
		t.Fatalf("duplicate pipe registration: added=%v err=%v", added, err)
	}
	loaded, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Pipes) != 1 {
		t.Fatalf("expected one pipe registration, got %d", len(loaded.Pipes))
	}
}

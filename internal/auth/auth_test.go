package auth

import (
	"os"
	"testing"
)

func TestParseClientTokensFile(t *testing.T) {
	path := t.TempDir() + "/client-tokens"
	if err := writeTokenFile(path, "client-a=token-a\nclient-b=token-b\n"); err != nil {
		t.Fatal(err)
	}
	tokens, err := ParseClientTokensFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if tokens["client-a"] != "token-a" || tokens["client-b"] != "token-b" {
		t.Fatalf("unexpected tokens: %#v", tokens)
	}
}

func writeTokenFile(path, contents string) error {
	return os.WriteFile(path, []byte(contents), 0o600)
}

// Package auth verifies the client identity and bearer token sent to
// pipe-server.
package auth

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
)

const ClientHeader = "X-AIPipe-Client-ID"

var (
	ErrMissingIdentity  = errors.New("client identity is required")
	ErrIdentityMismatch = errors.New("client identity does not match request")
	ErrInvalidToken     = errors.New("invalid bearer token")
)

type Verifier struct {
	tokens      map[string]string
	legacyToken string
}

func New(tokens map[string]string, legacyToken string) *Verifier {
	copyTokens := make(map[string]string, len(tokens))
	for clientID, token := range tokens {
		copyTokens[clientID] = token
	}
	return &Verifier{tokens: copyTokens, legacyToken: legacyToken}
}

func ParseClientTokens(raw string) (map[string]string, error) {
	result := map[string]string{}
	if strings.TrimSpace(raw) == "" {
		return result, nil
	}
	for _, item := range strings.Split(raw, ",") {
		parts := strings.SplitN(item, "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid client token entry %q", item)
		}
		result[strings.TrimSpace(parts[0])] = parts[1]
	}
	return result, nil
}

// ParseClientTokensFile reads client_id=token pairs from a file without
// exposing the file contents to logs or command output.
func ParseClientTokensFile(path string) (map[string]string, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read client token file: %w", err)
	}
	raw := strings.TrimSpace(string(contents))
	raw = strings.ReplaceAll(raw, "\r\n", ",")
	raw = strings.ReplaceAll(raw, "\n", ",")
	raw = strings.ReplaceAll(raw, "\r", ",")
	return ParseClientTokens(raw)
}

func (v *Verifier) Verify(r *http.Request, clientID string) error {
	headerID := r.Header.Get(ClientHeader)
	if headerID == "" || clientID == "" {
		return ErrMissingIdentity
	}
	if headerID != clientID {
		return ErrIdentityMismatch
	}

	token := bearerToken(r.Header.Get("Authorization"))
	if expected, configured := v.tokens[clientID]; configured {
		if !sameSecret(expected, token) {
			return ErrInvalidToken
		}
		return nil
	}
	if len(v.tokens) > 0 {
		return ErrInvalidToken
	}
	if v.legacyToken != "" && !sameSecret(v.legacyToken, token) {
		return ErrInvalidToken
	}
	return nil
}

func bearerToken(header string) string {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(header, prefix))
}

func sameSecret(expected, actual string) bool {
	if len(expected) != len(actual) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(expected), []byte(actual)) == 1
}

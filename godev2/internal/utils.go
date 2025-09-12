package internal

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
)

func TempFileName(prefix, suffix string) (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	randomStr := hex.EncodeToString(b)
	name := filepath.Join(os.TempDir(), prefix+randomStr+suffix)
	return name, nil
}

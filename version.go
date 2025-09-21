package physalis

import (
	"crypto/sha256"
	"encoding/base64"
	"io"
	"io/fs"
)

// VersionFromFS returns a deterministic "version" string for the contents
// of the provided filesystem (fs.FS). The version is derived from every file's
// bytes together with its relative path, so any change to file contents or to
// the set of files (add/remove/rename) results in a different version.
//
// Intended for use with embed.FS, but works with any fs.FS.
//
// On any traversal or read error, the function panics.
func VersionFromFS(fsys fs.FS) string {
	h := sha256.New()

	err := fs.WalkDir(fsys, ".", func(path string,
		d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		h.Write([]byte(path))
		h.Write([]byte{0})

		if d.IsDir() {
			return nil
		}
		if d.Type()&fs.ModeSymlink != 0 {
			return nil
		}

		f, err := fsys.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		_, err = io.Copy(h, f)
		if err != nil {
			return err
		}

		h.Write([]byte{255})

		return nil
	})

	if err != nil {
		panic(err)
	}

	return base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}

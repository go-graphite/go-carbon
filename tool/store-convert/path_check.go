package main

import (
	"errors"
	"path/filepath"
)

func validateSourcePath(source, relative string) error {
	if !filepath.IsLocal(relative) {
		return errors.New("source path is not local")
	}
	path := filepath.Join(source, relative)
	plain, err := provePlainSourcePath(path)
	if err != nil || plain {
		return err
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	if !within(source, resolved) {
		return errors.New("source path resolves outside the source tree")
	}
	return nil
}

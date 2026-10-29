//go:build !linux

package main

func provePlainSourcePath(string) (bool, error) { return false, nil }

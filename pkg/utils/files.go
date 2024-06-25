package utils

import "os"

func FileExists(path string) bool {
	stat, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		return false
	}
	if stat.IsDir() {
		return false
	}
	return true
}

func DirExists(path string) bool {
	stat, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		return false
	}
	if !stat.IsDir() {
		return false
	}
	return true
}

func FileOrDirExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}

func WithTrailingSlash(path string) string {
	if len(path) == 0 {
		return "./"
	}

	if path[len(path)-1] != '/' {
		return path + "/"
	}
	return path
}

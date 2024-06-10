package commands

import (
	"fisi/elenadb/pkg/utils"
	"fmt"
	"io/fs"
	"os"
)

// NOTE: (!) This function has no sense. Database creation will be performed with
// the "creame" command. This function will be removed in the future.
func ElenaCreate(name string, ignoreExists bool) (bool, error) {
	if !ignoreExists && utils.FileOrDirExists(name) {
		return false, fmt.Errorf("db %s already exists", name)
	}

	err := os.Mkdir(name, fs.FileMode(0755))
	if os.IsExist(err) {
		return false, nil
	}
	return true, err
}

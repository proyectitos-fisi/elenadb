package storage

import (
	"fmt"
	"math/rand"
	"os"
	"time"
)

func randomInt() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(1000000) // Generates a random integer between 0 and 999999
}

// Trying files for the first time in go
// WriteToFile writes text to a file.
func WriteToFile(filename, text string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(text)
	if err != nil {
		return err
	}

	return nil
}

func SaveData1(path string, data []byte) error {
	fp, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		return err
	}
	defer fp.Close()

	_, err = fp.Write(data)
	if err != nil {
		return err
	}
	return fp.Sync() // fsync
}

func SaveData2(path string, data []byte) error {
	tmp := fmt.Sprintf("%s.tmp.%d", path, randomInt())
	fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0664)
	if err != nil {
		return err
	}

	defer func() {
		if closeErr := fp.Close(); closeErr != nil {
			fmt.Printf("Error closing file: %v\n", closeErr)
		}
		if err != nil {
			os.Remove(tmp)
		}
	}()

	_, err = fp.Write(data)
	if err != nil {
		return err
	}
	err = fp.Sync() // fsync
	if err != nil {
		return err
	}

	// Close the file before renaming
	if closeErr := fp.Close(); closeErr != nil {
		fmt.Printf("Error closing file: %v\n", closeErr)
		return closeErr
	}

	return os.Rename(tmp, path)
}

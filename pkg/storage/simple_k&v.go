package storage

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
)

const FileName = "data.txt"

func Testin_k_v() {
	if err := insert_kv("apple", "A fruit"); err != nil {
		fmt.Println("Error inserting:", err)
	}
	if err := insert_kv("banana", "Another fruit"); err != nil {
		fmt.Println("Error inserting:", err)
	}
	if err := insert_kv("cat", "An animal"); err != nil {
		fmt.Println("Error inserting:", err)
	}

	fmt.Println(retrieve_kv("apple"))
	fmt.Println(retrieve_kv("banana"))
	fmt.Println(retrieve_kv("cat"))
	fmt.Println(retrieve_kv("dog"))
}

func insert_kv(key, value string) error {
	file, err := os.OpenFile(FileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(fmt.Sprintf("%s:%s\n", key, value))
	return err
}

func retrieve_kv(key string) (string, error) {
	file, err := os.Open(FileName)
	if err != nil {
		return "", err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 && parts[0] == key {
			return parts[1], nil
		}
	}

	return "", errors.New("key not found")
}

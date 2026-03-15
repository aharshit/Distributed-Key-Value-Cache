package utils

import "os"

func WriteToFile(data []byte, file *os.File) error {
	// panic on write errors
	if _, writeErr := file.Write(data); writeErr != nil {
		panic(writeErr)
	}
	// Sync flushes buffer to disk to persist data
	if syncErr := file.Sync(); syncErr != nil {
		panic(syncErr)
	}
	return nil
}

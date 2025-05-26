package snerd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var compacting atomic.Bool

// FileStore provides persistent storage for retryable tasks.
// It manages task log files, compaction, and metadata tracking for tasks.
type FileStore struct {
	mu           sync.Mutex
	filePath     string
	totalTasks   int
	deletedTasks int
	appendCount  int
}

// NewFileStore creates a new FileStore for the given file path.
// It rebuilds metadata from the existing log file if present.
func NewFileStore(path string) (*FileStore, error) {
	fs := &FileStore{
		filePath: path,
	}

	// rebuild counters from existing task.log file.
	if err := fs.RebuildMetaData(); err != nil {
		return nil, fmt.Errorf("rebuild metadata: %w", err)
	}

	return fs, nil
}

// RebuildMetaData scans the log file and rebuilds internal counters for tasks and deletions.
func (fs *FileStore) RebuildMetaData() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	file, err := os.Open(fs.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// nothing to rebuild.
			return nil
		}
		return fmt.Errorf("open file: %w", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Printf("Error closing file: %s\n", err)
			return
		}
	}(file)

	fs.totalTasks = 0
	fs.deletedTasks = 0
	fs.appendCount = 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()
		var t RetryableTask
		if err := json.Unmarshal(line, &t); err != nil {
			fmt.Printf("unmarshal task: %v\n", err)
			continue
		}

		fs.appendCount++
		if t.DeletedAt != nil {
			fs.deletedTasks++
		} else {
			fs.totalTasks++
		}

	}
	return scanner.Err()
}

// CreateTask appends a new retryable task to the log file and updates internal counters.
func (fs *FileStore) CreateTask(task *RetryableTask) error {
	fmt.Println("Creating task:", task)
	// get the mutex lock and unlock out of the way
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Ensure parent directory exists before creating or appending to the file
	dirPath := filepath.Dir(fs.filePath)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}

	// Make sure the .snerdata directory is hidden on Windows
	// On Unix-like systems, directories starting with a dot are already hidden
	if runtime.GOOS == "windows" {
		// For Windows, we'll use the attrib command to hide the directory
		// Get the parent directory of our tasks folder to find the .snerdata folder
		snerDataDir := filepath.Join(filepath.Dir(dirPath), ".snerdata")
		// Use attrib command to set the hidden attribute
		cmd := exec.Command("attrib", "+h", snerDataDir)
		if err := cmd.Run(); err != nil {
			// Just log the error but don't fail - hiding is a nice-to-have
			fmt.Printf("Warning: Could not hide directory on Windows: %v\n", err)
		}
	}

	fmt.Println("Opening file:", fs.filePath)
	f, err := os.OpenFile(fs.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	fmt.Println("Opened file:", f)
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			fmt.Printf("Error closing file: %s\n", err)
			return
		}
	}(f)

	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal task: %w", err)
	}
	fmt.Println("Writing task to file:", string(data))
	_, err = f.Write(append(data, '\n'))
	if err != nil {
		return fmt.Errorf("write task: %w", err)
	}
	fmt.Println("Wrote task to file")
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync task: %w", err)
	}
	fmt.Println("Synced task to file")

	// Track stats
	fs.appendCount++
	if task.DeletedAt != nil {
		fs.deletedTasks++
	} else {
		fs.totalTasks++
	}
	fmt.Println("Updated stats:", fs)

	// Check if compaction should be triggered
	if fs.shouldCompact() {
		fmt.Println("Triggering compaction")
		go func() {
			fmt.Println("Compacting file")
			err := fs.Compact()
			if err != nil {
				fmt.Printf("Error compacting file: %s\n", err)
			}
			fmt.Println("Compacted file")
		}()

	}
	fmt.Println("Done with compaction")
	return nil

}

func (fs *FileStore) ReadTasks() ([]*RetryableTask, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Make sure the parent directory exists
	dirPath := filepath.Dir(fs.filePath)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", dirPath, err)
	}

	// Try to open the file
	f, err := os.Open(fs.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// No tasks file yet, return empty slice
			fmt.Printf("Task file does not exist yet at %s, returning empty list\n", fs.filePath)
			return []*RetryableTask{}, nil
		}
		return nil, fmt.Errorf("failed to open task file %s: %w", fs.filePath, err)
	}

	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			fmt.Printf("Error closing file %s: %v\n", fs.filePath, err)
			return
		}
	}(f)

	// Build map of latest tasks by ID
	taskMap := make(map[string]*RetryableTask)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		var t RetryableTask
		if err := json.Unmarshal(line, &t); err != nil {
			fmt.Printf("unmarshal task: %v\n", err)
			continue
		}

		// Skip deleted tasks
		if t.DeletedAt != nil {
			delete(taskMap, t.TaskID)
			continue
		}

		// Store the latest version of the task
		taskMap[t.TaskID] = &t
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan file: %w", err)
	}

	// Convert map to slice
	tasks := make([]*RetryableTask, 0, len(taskMap))
	for _, task := range taskMap {
		tasks = append(tasks, task)
	}

	return tasks, nil
}

// ReadDueTasks returns all tasks that are currently in the store that are due for execution
// This filters out deleted tasks and returns only active tasks whose retry time has passed
func (fs *FileStore) ReadDueTasks() ([]*RetryableTask, error) {
	// Get all active tasks first
	tasks, err := fs.ReadTasks()
	if err != nil {
		return nil, fmt.Errorf("read tasks: %w", err)
	}

	// Filter tasks that are due for execution (their retry time has passed)
	now := time.Now()
	dueTasks := make([]*RetryableTask, 0, len(tasks))

	for _, task := range tasks {
		if task.RetryAfterTime.Before(now) || task.RetryAfterTime.Equal(now) {
			dueTasks = append(dueTasks, task)
		}
	}

	if len(dueTasks) > 0 {
		fmt.Printf("Found %d tasks due for execution out of %d total tasks\n",
			len(dueTasks), len(tasks))
	}

	return dueTasks, nil
}

func (fs *FileStore) UpdateTaskRetryConfig(taskID string, errorObj error) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	f, err := os.Open(fs.filePath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}

	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			fmt.Printf("Error closing file: %s\n", err)
			return
		}
	}(f)

	var latest *RetryableTask
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := scanner.Bytes()
		var t RetryableTask
		if err := json.Unmarshal(line, &t); err != nil {
			fmt.Printf("unmarshal task: %v\n", err)
			continue
		}

		if t.TaskID == taskID {
			latest = &t
		}

	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan file: %w", err)
	}

	if latest == nil {
		return fmt.Errorf("task with ID %s not found", taskID)
	}

	if latest.DeletedAt != nil {
		return fmt.Errorf("cannot update a deleted task: %s", taskID)
	}

	latest.RetryCount++
	latest.RetryAfterTime = time.Now().Add(time.Duration(latest.RetryAfterHours) * time.
		Hour)

	// Store error information if provided
	if errorObj != nil {
		// Create a JobErrorReturn for structured error data
		latest.LastJobError = &JobErrorReturn{
			ErrorObj:    errorObj,
			RetryWorthy: true, // Assuming it's retry-worthy since we're updating retry config
		}

		// Store error message as a string in TaskData for easier debugging
		if latest.TaskData != "" {
			// Try to parse existing data to add error information
			var data map[string]interface{}
			if err := json.Unmarshal([]byte(latest.TaskData), &data); err == nil {
				// Add error information
				data["lastError"] = errorObj.Error()
				data["lastErrorTime"] = time.Now().Format(time.RFC3339)

				// Re-serialize
				if updatedData, err := json.Marshal(data); err == nil {
					latest.TaskData = string(updatedData)
				}
			}
		}
	}
	fmt.Println("Got here!!!!")
	return fs.CreateTask(latest)

}

// We use a soft deleting approach alongside compaction
// getLatestTask returns the most recent version of a task by its ID
// It scans the entire task log to find the most up-to-date version
func (fs *FileStore) getLatestTask(taskID string) (*RetryableTask, error) {
	file, err := os.Open(fs.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("task %s not found: log file does not exist", taskID)
		}
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Printf("Error closing file: %s\n", err)
		}
	}(file)

	scanner := bufio.NewScanner(file)
	var latest *RetryableTask

	// Scan through the file to find the latest version of the task
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		var t RetryableTask
		if err := json.Unmarshal([]byte(line), &t); err != nil {
			fmt.Printf("unmarshal task: %v\n", err)
			continue
		}

		// If this is the task we're looking for, update our latest reference
		if t.TaskID == taskID {
			latest = &t
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan file: %w", err)
	}

	if latest == nil {
		return nil, fmt.Errorf("task with ID %s not found", taskID)
	}

	return latest, nil
}

func (fs *FileStore) DeleteTask(taskID string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// First find the existing task to preserve its data
	existingTask, err := fs.getLatestTask(taskID)
	if err != nil {
		// If the task doesn't exist, that's not an error - it might have been deleted already
		if strings.Contains(err.Error(), "not found") {
			fmt.Printf("Task %s already deleted or not found\n", taskID)
			return nil
		}
		return fmt.Errorf("cannot delete task, error retrieving it: %w", err)
	}

	// Check if already deleted
	if existingTask.DeletedAt != nil {
		fmt.Printf("Task %s already marked as deleted\n", taskID)
		return nil
	}

	// Mark the task as deleted while preserving all other data
	now := time.Now()
	existingTask.DeletedAt = &now

	// Marshal to JSON
	data, err := json.Marshal(existingTask)
	if err != nil {
		return fmt.Errorf("marshal task for deletion: %w", err)
	}
	fmt.Println("TASK TO BE DELETED: ", existingTask)
	// Open the file for appending
	f, err := os.OpenFile(fs.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			fmt.Printf("Error closing file: %s\n", err)
			return
		}
	}(f)

	// Append the JSON entry followed by a newline
	if _, err := f.Write(append(data, '\n')); err != nil {
		fmt.Println("Error writing to file:", err)
		return fmt.Errorf("write to file: %w", err)
	}

	// Track stats
	fs.appendCount++
	fs.deletedTasks++ // This is a delete operation, so increment the deletedTasks counter

	// Check if compaction should be triggered
	if fs.shouldCompact() {
		fmt.Println("COMPACTING THE FILE!!!!")
		go func() {
			err := fs.Compact()
			if err != nil {
				fmt.Printf("Error compacting file: %s\n", err)
			}
		}()

	}

	return nil
}

// Compact removes all deleted tasks from the log file to reduce its size.
// This method was previously called CompactLog but is now renamed to Compact
// for consistency with other methods.
func (fs *FileStore) Compact() error {
	// Use atomic flag to ensure only one compaction runs at a time
	if !compacting.CompareAndSwap(false, true) {
		fmt.Println("Task log compaction already in progress, skipping")
		return nil // Already compacting
	}
	defer compacting.Store(false)

	// Log compaction start
	start := time.Now()
	fmt.Printf("Starting task log compaction at %s\n", start.Format(time.RFC3339))

	// Lock the mutex for the entire compaction process
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Create a temporary file for the compacted log
	tempFilePath := fs.filePath + ".tmp"
	tempFile, err := os.Create(tempFilePath)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer func(tempFile *os.File) {
		err := tempFile.Close()
		if err != nil {
			fmt.Printf("Error closing temp file during compaction: %v\n", err)
			return
		}
	}(tempFile)

	// Stats for logging
	var deletedTaskCount int

	fmt.Println("Building latest state of each task")
	// Build the latest state of each task
	taskMap := make(map[string]*RetryableTask)
	deleted := make(map[string]bool)

	inputFile, err := os.Open(fs.filePath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			fmt.Printf("Error closing file: %s\n", err)
			return
		}
	}(inputFile)

	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		line := scanner.Bytes()
		var t RetryableTask
		if err := json.Unmarshal(line, &t); err != nil {
			fmt.Printf("unmarshal task: %v\n", err)
			continue
		}

		// Track deleted tasks
		if t.DeletedAt != nil {
			deleted[t.TaskID] = true
			deletedTaskCount++
			continue
		}

		// Keep only the latest version of each task
		if existing, ok := taskMap[t.TaskID]; !ok || t.UpdatedAt.After(existing.UpdatedAt) {
			taskMap[t.TaskID] = &t
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan file: %w", err)
	}
	fmt.Println("Scanned file")
	// Already created temp file at the beginning, now we'll use it
	fmt.Println("Writing to temp file:", tempFile.Name())
	encoder := json.NewEncoder(tempFile)
	for _, task := range taskMap {
		if err := encoder.Encode(task); err != nil {
			err := tempFile.Close()
			if err != nil {
				return err
			}
			return fmt.Errorf("encode task: %w", err)
		}
	}

	// flush tempfile to disk
	fmt.Println("Flushing tempfile to disk")
	if err := tempFile.Sync(); err != nil {
		err := tempFile.Close()
		if err != nil {
			return err
		}
		return fmt.Errorf("sync tempfile: %w", err)
	}

	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("close tempfile: %w", err)
	}
	fmt.Println("Closed tempfile")
	// finally atomically replace old file
	fmt.Println("Renaming tempfile to file")
	if err := os.Rename(tempFilePath, fs.filePath); err != nil {
		return fmt.Errorf("rename tempfile: %w", err)
	}
	fmt.Println("Renamed tempfile to file")

	return nil

}

func (fs *FileStore) shouldCompact() bool {
	// Caller must hold lock
	info, err := os.Stat(fs.filePath)
	if err != nil {
		log.Printf("Failed to stat file: %v", err)
		return false // Be conservative or true based on your compaction model
	}

	// Check file size
	if info.Size() > 20*1024*1024 {
		log.Printf("Compaction triggered by file size: %d bytes", info.Size())
		return true
	}

	// Check deleted ratio
	if fs.totalTasks > 0 {
		ratio := float64(fs.deletedTasks) / float64(fs.totalTasks)
		if ratio > 0.5 {
			log.Printf("Compaction triggered by deleted task ratio: %.2f", ratio)
			return true
		}
	}

	// Check append count
	if fs.appendCount >= 10000 {
		log.Println("Compaction triggered by append count")
		fs.appendCount = 0
		return true
	}

	return false
}

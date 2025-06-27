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
		if t.DeletedAt != nil && !t.DeletedAt.IsZero() {
			fs.deletedTasks++
		} else {
			fs.totalTasks++
		}

	}
	return scanner.Err()
}

// CreateTask appends a new retryable task to the log file and updates internal counters.
func (fs *FileStore) CreateTask(task *RetryableTask) error {
	log.Printf("[CreateTask] Called for taskId=%s retryCount=%d deletedAt=%v\n", task.TaskID, task.RetryCount, task.DeletedAt)

	fmt.Printf("Creating task: ID=%s RetryCount=%d RetryAfterTime=%v TaskType=%s\n", task.TaskID, task.RetryCount, task.RetryAfterTime, task.TaskType)

	// get the mutex lock and unlock out of the way
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Ensure parent directory exists before creating or appending to the file
	dirPath := filepath.Dir(fs.filePath)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		log.Printf("Error dey here!!!!! %v\n", err)
		return fmt.Errorf("create directory: %w", err)
	}

	// Make sure the .snerdata directory is hidden on Windows
	// On Unix-like systems, directories starting with a dot are already hidden
	if runtime.GOOS == "windows" {
		fmt.Println("I DOUBT IT IS HERE FOR WINDOWS!!!!!!")
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

	log.Printf("[CreateTask] Opening file: %s\n", fs.filePath)
	f, err := os.OpenFile(fs.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("Error dey here 111 !!!!! %v\n", err)
		return fmt.Errorf("open file: %w", err)
	}
	log.Printf("[CreateTask] Opened file: %v\n", f)
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			fmt.Printf("Error closing file: %s\n", err)
			return
		}
	}(f)

	data, err := json.Marshal(task)
	if err != nil {
		log.Printf("[CreateTask] ERROR marshaling task %s: %v\n", task.TaskID, err)
		return fmt.Errorf("marshal task: %w", err)
	}

	log.Printf("[CreateTask] Writing task to file: %s\n", string(data))
	log.Printf("[CreateTask] File path: %s\n", fs.filePath)
	log.Printf("[CreateTask] File descriptor: %v\n", f.Fd())

	n, err := f.Write(append(data, '\n'))
	if err != nil {
		fileInfo, statErr := f.Stat()
		if statErr != nil {
			log.Printf("[CreateTask] ERROR getting file info: %v\n", statErr)
		} else {
			log.Printf("[CreateTask] File info: %+v\n", fileInfo)
		}
		log.Printf("[CreateTask] ERROR writing task to file: %v (wrote %d bytes)\n", err, n)
		return fmt.Errorf("write task: %w (wrote %d bytes)", err, n)
	}
	log.Printf("[CreateTask] Successfully wrote %d bytes to file: %s\n", n, fs.filePath)

	log.Printf("Creating task: ID=%s RetryCount=%d RetryAfterTime=%v TaskType=%s\n",
		task.TaskID, task.RetryCount, task.RetryAfterTime, task.TaskType)

	if err := f.Sync(); err != nil {
		log.Printf("[CreateTask] ERROR syncing file: %v\n", err)
		return fmt.Errorf("sync task: %w", err)
	}
	log.Printf("[CreateTask] Successfully synced file: %s\n", fs.filePath)

	// Track stats
	fs.appendCount++
	if task.DeletedAt != nil && !task.DeletedAt.IsZero() {
		fs.deletedTasks++
	} else {
		fs.totalTasks++
	}
	log.Printf("[CreateTask] Updated stats: totalTasks=%d deletedTasks=%d appendCount=%d\n", fs.totalTasks, fs.deletedTasks, fs.appendCount)

	//// Check if compaction should be triggered
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
	defer f.Close()

	// Build map of latest tasks by ID
	taskMap := make(map[string]*RetryableTask)
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue // Skip empty lines
		}

		t := &RetryableTask{} // Create a new instance for each task
		if err := json.Unmarshal(line, t); err != nil {
			fmt.Printf("Error unmarshaling task: %v\n", err)
			continue
		}

		// Skip deleted tasks
		if t.DeletedAt != nil && !t.DeletedAt.IsZero() {
			fmt.Printf("[ReadTasks] Skipping deleted task: %s (deletedAt=%v)\n", t.TaskID, t.DeletedAt)
			delete(taskMap, t.TaskID)
			continue
		}

		// Create a new instance to avoid pointer issues
		taskCopy := *t
		taskMap[t.TaskID] = &taskCopy
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan file: %w", err)
	}

	// Convert map to slice
	tasks := make([]*RetryableTask, 0, len(taskMap))
	for _, task := range taskMap {
		if task.DeletedAt == nil || task.DeletedAt.IsZero() {
			tasks = append(tasks, task)
		}
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
		if (task.DeletedAt == nil || task.DeletedAt.IsZero()) && (task.RetryAfterTime.Before(now) || task.RetryAfterTime.Equal(now)) {
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
	log.Printf("[UpdateTaskRetryConfig] Called for taskId=%s\n", taskID)

	// Get the latest version of the task using the helper method
	fs.mu.Lock()
	latest, err := fs.getLatestTask(taskID)
	if err != nil {
		fs.mu.Unlock()
		log.Printf("[UpdateTaskRetryConfig] ERROR: %v\n", err)
		return fmt.Errorf("get latest task: %w", err)
	}

	// Check if the task is deleted
	if latest.DeletedAt != nil && !latest.DeletedAt.IsZero() {
		fs.mu.Unlock()
		errMsg := fmt.Sprintf("cannot update a deleted task: %s", taskID)
		log.Printf("[UpdateTaskRetryConfig] ERROR: %s\n", errMsg)
		return fmt.Errorf(errMsg)
	}

	// Create a deep copy of the task to avoid data races
	updatedTask := *latest
	fs.mu.Unlock() // Release the mutex before calling CreateTask

	// Update the task
	updatedTask.RetryCount = updatedTask.RetryCount + 1
	updatedTask.RetryAfterTime = time.Now().Add(time.Duration(updatedTask.RetryAfterHours) * time.Hour)

	// Store error information if provided
	if errorObj != nil {
		// Create a JobErrorReturn for structured error data
		updatedTask.LastJobError = &JobErrorReturn{
			ErrorObj:    errorObj,
			ErrorString: errorObj.Error(),
			RetryWorthy: true, // Assuming it's retry-worthy since we're updating retry config
		}

		// Store error message as a string in TaskData for easier debugging
		if updatedTask.TaskData != "" {
			var data map[string]interface{}
			if err := json.Unmarshal([]byte(updatedTask.TaskData), &data); err == nil {
				// Add error information
				data["lastError"] = errorObj.Error()
				data["lastErrorTime"] = time.Now().Format(time.RFC3339)
				data["retryCount"] = updatedTask.RetryCount
				data["retryAfterTime"] = updatedTask.RetryAfterTime.Format(time.RFC3339)

				// Re-serialize
				if updatedData, err := json.Marshal(data); err == nil {
					updatedTask.TaskData = string(updatedData)
				}
			}
		}
	}

	log.Printf("[UpdateTaskRetryConfig] About to call CreateTask for retried taskId=%s retryCount=%d\n",
		updatedTask.TaskID, updatedTask.RetryCount)
	return fs.CreateTask(&updatedTask)
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

		t := &RetryableTask{} // Create a new instance for each task
		if err := json.Unmarshal([]byte(line), t); err != nil {
			fmt.Printf("unmarshal task: %v\n", err)
			continue
		}

		// If this is the task we're looking for, update our latest reference
		if t.TaskID == taskID {
			// Create a new instance to avoid pointer issues
			taskCopy := *t
			latest = &taskCopy
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
	if existingTask.DeletedAt != nil && !existingTask.DeletedAt.IsZero() {
		fmt.Printf("Task %s already marked as deleted\n", taskID)
		return nil
	}

	// Create a new task with the same data but marked as deleted
	deletedTask := *existingTask // Create a copy
	now := time.Now()
	deletedTask.DeletedAt = &now

	// Debug: Print DeletedAt before marshaling
	if deletedTask.DeletedAt != nil {
		fmt.Printf("[DeleteTask] About to delete task %s at %v\n", deletedTask.TaskID, *deletedTask.DeletedAt)
	} else {
		fmt.Printf("[DeleteTask] About to delete task %s but DeletedAt is nil!\n", deletedTask.TaskID)
	}

	// Marshal to JSON
	data, err := json.Marshal(&deletedTask)
	if err != nil {
		return fmt.Errorf("marshal task for deletion: %w", err)
	}

	fmt.Printf("[DeleteTask] Marshaled JSON for deleted task %s: %s\n", deletedTask.TaskID, string(data))

	// Open the file for appending
	f, err := os.OpenFile(fs.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}

	// Write the deleted task to the log
	if _, err := f.Write(append(data, '\n')); err != nil {
		f.Close()
		return fmt.Errorf("write to file: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close file: %w", err)
	}

	// Update stats
	fs.appendCount++
	fs.deletedTasks++

	// Check if compaction should be triggered
	if fs.shouldCompact() {
		fmt.Println("Compacting task log file...")
		go func() {
			if err := fs.Compact(); err != nil {
				fmt.Printf("Error compacting file: %v\n", err)
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

	// Stats for logging
	var (
		deletedTaskCount int
		totalTasks       int
	)

	// Build the latest state of each task
	taskMap := make(map[string]*RetryableTask)

	inputFile, err := os.Open(fs.filePath)
	if err != nil {
		tempFile.Close()
		return fmt.Errorf("open file: %w", err)
	}

	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue // Skip empty lines
		}

		totalTasks++
		t := &RetryableTask{} // Create a new instance for each task
		if err := json.Unmarshal(line, t); err != nil {
			fmt.Printf("Error unmarshaling task during compaction: %v\n", err)
			continue
		}

		// Track deleted tasks
		if t.DeletedAt != nil && !t.DeletedAt.IsZero() {
			deletedTaskCount++
			continue
		}

		// Keep only the latest version of each task
		if existing, ok := taskMap[t.TaskID]; !ok || t.UpdatedAt.After(existing.UpdatedAt) {
			taskMap[t.TaskID] = t
		}
	}

	// Close the input file
	if err := inputFile.Close(); err != nil {
		tempFile.Close()
		return fmt.Errorf("close input file: %w", err)
	}

	if err := scanner.Err(); err != nil {
		tempFile.Close()
		return fmt.Errorf("scan file: %w", err)
	}

	// Write compacted tasks to temp file
	encoder := json.NewEncoder(tempFile)
	for _, task := range taskMap {
		// Skip any remaining deleted tasks (shouldn't happen due to earlier check)
		if task.DeletedAt != nil && !task.DeletedAt.IsZero() {
			continue
		}

		if err := encoder.Encode(task); err != nil {
			tempFile.Close()
			return fmt.Errorf("encode task: %w", err)
		}
	}

	// Flush to disk
	if err := tempFile.Sync(); err != nil {
		tempFile.Close()
		return fmt.Errorf("sync temp file: %w", err)
	}

	// Close the temp file before renaming
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}

	// Atomically replace the old file with the new one
	if err := os.Rename(tempFilePath, fs.filePath); err != nil {
		return fmt.Errorf("rename temp file: %w", err)
	}

	// Update stats
	fs.totalTasks = len(taskMap)
	fs.deletedTasks = 0
	fs.appendCount = 0

	// Log completion
	duration := time.Since(start)
	fmt.Printf("Compaction completed in %v. Removed %d/%d tasks\n",
		duration.Round(time.Millisecond), deletedTaskCount, totalTasks)

	return nil
}

func (fs *FileStore) shouldCompact() bool {
	// Caller must hold lock
	info, err := os.Stat(fs.filePath)
	if err != nil {
		log.Printf("Failed to stat file: %v\n", err)
		return false // Be conservative or true based on your compaction model
	}

	// Check file size
	if info.Size() > 20*1024*1024 {
		log.Printf("Compaction triggered by file size: %d bytes\n", info.Size())
		return true
	}

	// Check deleted ratio
	if fs.totalTasks > 0 {
		ratio := float64(fs.deletedTasks) / float64(fs.totalTasks)
		if ratio > 0.5 {
			log.Printf("Compaction triggered by deleted task ratio: %.2f\n", ratio)
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

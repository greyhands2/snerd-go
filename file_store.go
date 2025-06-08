package snerd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math"
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

	// Try to open the file
	f, err := os.Open(fs.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return []*RetryableTask{}, nil // Return empty slice if file doesn't exist
		}
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	taskMap := make(map[string]*RetryableTask)
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue // Skip empty lines
		}

		t := &RetryableTask{} // Create a new instance for each task
		if err := json.Unmarshal(line, &t); err != nil {
			fmt.Printf("Error unmarshaling task: %v\n", err)
			continue
		}

		// Check if task is marked as deleted in the main struct
		if t.DeletedAt != nil && !t.DeletedAt.IsZero() {
			continue
		}

		// Check for deletion markers in TaskData
		if t.TaskData != "" {
			var taskData map[string]interface{}
			if err := json.Unmarshal([]byte(t.TaskData), &taskData); err == nil {
				// Check all possible deletion markers
				if deletedAt, ok := taskData["deletedAt"].(string); ok && deletedAt != "" {
					continue
				}
				if deleted, ok := taskData["deleted"].(bool); ok && deleted {
					continue
				}
				if status, ok := taskData["status"].(string); ok && status == "deleted" {
					continue
				}
			}
		}

		// Also check if deleted in TaskData for backward compatibility
		if t.TaskData != "" {
			var taskData map[string]interface{}
			if err := json.Unmarshal([]byte(t.TaskData), &taskData); err == nil {
				if deletedAt, ok := taskData["deletedAt"].(string); ok && deletedAt != "" {
					continue
				}
			}
		}

		// Keep only the latest version of each task
		if existing, ok := taskMap[t.TaskID]; !ok || t.UpdatedAt.After(existing.UpdatedAt) {
			taskMap[t.TaskID] = t
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan file: %w", err)
	}

	// Convert map to slice and create copies to avoid pointer issues
	result := make([]*RetryableTask, 0, len(taskMap))
	for _, task := range taskMap {
		taskCopy := *task
		result = append(result, &taskCopy)
	}

	return result, nil
}

// ReadDueTasks returns all tasks that are currently in the store that are due for execution
// This filters out deleted tasks and returns only active tasks whose retry time has passed
func (fs *FileStore) ReadDueTasks() ([]*RetryableTask, error) {
	tasks, err := fs.ReadTasks()
	if err != nil {
		return nil, fmt.Errorf("read tasks: %w", err)
	}

	now := time.Now()
	dueTasks := make([]*RetryableTask, 0, len(tasks))

	for _, task := range tasks {
		// Check if task is due
		if !task.RetryAfterTime.IsZero() {
			if task.RetryAfterTime.After(now) {
				fmt.Printf("Skipping task %s: not due yet (due at %v, now is %v)\n", task.TaskID, task.RetryAfterTime, now)
				continue
			}
			// If we get here, task.RetryAfterTime is in the past, so it's due
			fmt.Printf("Task %s is due (was due at %v, now is %v)\n", task.TaskID, task.RetryAfterTime, now)
		} else {
			// Zero time means task is new and should be processed immediately
			fmt.Printf("Task %s is new (no retry time set), processing now\n", task.TaskID)
		}

		// Double-check that task is not marked as deleted in TaskData
		if task.TaskData != "" {
			var taskData map[string]interface{}
			if err := json.Unmarshal([]byte(task.TaskData), &taskData); err == nil {
				// Check all possible deletion markers
				if deletedAt, ok := taskData["deletedAt"].(string); ok && deletedAt != "" {
					continue
				}
				if deleted, ok := taskData["deleted"].(bool); ok && deleted {
					continue
				}
				if status, ok := taskData["status"].(string); ok && status == "deleted" {
					continue
				}
			}
		}

		dueTasks = append(dueTasks, task)
	}

	if len(dueTasks) > 0 {
		fmt.Printf("Found %d tasks due for execution (out of %d total tasks)\n", len(dueTasks), len(tasks))
	}

	return dueTasks, nil
}

func (fs *FileStore) UpdateTaskRetryConfig(taskID string, errorObj error) error {
	log.Printf("[UpdateTaskRetryConfig] Called for taskId=%s\n", taskID)

	// Get the latest version of the task using the helper method
	fs.mu.Lock()
	defer fs.mu.Unlock()

	latest, err := fs.getLatestTask(taskID)
	if err != nil {
		log.Printf("[UpdateTaskRetryConfig] ERROR: %v\n", err)
		return fmt.Errorf("get latest task: %w", err)
	}

	// Check if the task is deleted
	if latest.DeletedAt != nil && !latest.DeletedAt.IsZero() {
		errMsg := fmt.Sprintf("cannot update a deleted task: %s", taskID)
		log.Printf("[UpdateTaskRetryConfig] ERROR: %s\n", errMsg)
		return fmt.Errorf(errMsg)
	}

	// Mark the old task as deleted by setting DeletedAt
	now := time.Now()
	latest.DeletedAt = &now
	latest.UpdatedAt = now

	// Persist the deletion of the old task
	log.Printf("[UpdateTaskRetryConfig] About to persist deleted old task: taskId=%s retryCount=%d deletedAt=%v\n", latest.TaskID, latest.RetryCount, latest.DeletedAt)
	if err := fs.CreateTask(latest); err != nil {
		log.Printf("[UpdateTaskRetryConfig] ERROR persisting deleted old task: %v\n", err)
		return fmt.Errorf("persist deleted old task: %w", err)
	}
	log.Printf("[UpdateTaskRetryConfig] Persisted deleted old task: taskId=%s retryCount=%d deletedAt=%v\n", latest.TaskID, latest.RetryCount, latest.DeletedAt)

	// Create a copy of the task to update
	updatedTask := *latest
	updatedTask.UpdatedAt = now
	updatedTask.DeletedAt = nil // Clear DeletedAt for the new version
	log.Printf("[UpdateTaskRetryConfig] About to append retry copy: taskId=%s retryCount=%d deletedAt=%v\n", updatedTask.TaskID, updatedTask.RetryCount, updatedTask.DeletedAt)

	// If this is a retry, update the retry count and schedule the next retry
	if errorObj != nil {
		updatedTask.RetryCount++
		updatedTask.LastJobError = &JobErrorReturn{
			ErrorObj:    errorObj,
			ErrorString: errorObj.Error(),
			RetryWorthy: true,
		}

		// Parse TaskData to update error info - preserve all existing data
		var taskData map[string]interface{}
		if updatedTask.TaskData != "" {
			if err := json.Unmarshal([]byte(updatedTask.TaskData), &taskData); err != nil {
				log.Printf("[UpdateTaskRetryConfig] Error unmarshaling TaskData: %v\n", err)
				taskData = make(map[string]interface{})
			}
		} else {
			taskData = make(map[string]interface{})
		}

		// Update task data with error and retry information
		taskData["lastError"] = errorObj.Error()
		taskData["lastErrorTime"] = now.Format(time.RFC3339)
		taskData["retryCount"] = updatedTask.RetryCount

		// Calculate next retry time with exponential backoff
		retryHours := updatedTask.RetryAfterHours
		if retryHours <= 0 {
			retryHours = 0.5 // Default to 30 minutes if not specified
		}

		// Apply exponential backoff based on retry count
		backoffFactor := math.Pow(2, float64(updatedTask.RetryCount-1))
		retryDuration := time.Duration(retryHours*backoffFactor*float64(time.Hour)) / time.Hour
		nextRetryTime := time.Now().Add(retryDuration)

		// Update task fields
		updatedTask.RetryAfterTime = nextRetryTime
		taskData["retryAfterTime"] = nextRetryTime.Format(time.RFC3339)

		// Update the TaskData JSON
		if updatedData, err := json.Marshal(taskData); err == nil {
			updatedTask.TaskData = string(updatedData)
			log.Printf("[UpdateTaskRetryConfig] Updated TaskData for task %s: %s\n",
				taskID, updatedTask.TaskData)
		} else {
			log.Printf("[UpdateTaskRetryConfig] Error marshaling TaskData: %v\n", err)
		}

		log.Printf("[UpdateTaskRetryConfig] Task %s will retry at %s (attempt %d/%d)\n",
			taskID, nextRetryTime.Format(time.RFC3339),
			updatedTask.RetryCount, updatedTask.MaxRetries)
	}

	log.Printf("[UpdateTaskRetryConfig] About to call CreateTask for taskId=%s retryCount=%d deletedAt=%v\n",
		updatedTask.TaskID, updatedTask.RetryCount, updatedTask.DeletedAt)
	errCreate := fs.CreateTask(&updatedTask)
	if errCreate != nil {
		log.Printf("[UpdateTaskRetryConfig] ERROR appending retry copy: %v\n", errCreate)
	} else {
		log.Printf("[UpdateTaskRetryConfig] Successfully appended retry copy: taskId=%s retryCount=%d deletedAt=%v\n", updatedTask.TaskID, updatedTask.RetryCount, updatedTask.DeletedAt)
	}
	return errCreate
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

	// Update the task data to include deletion info
	if deletedTask.TaskData != "" {
		var taskData map[string]interface{}
		if err := json.Unmarshal([]byte(deletedTask.TaskData), &taskData); err == nil {
			// Ensure all deletion markers are set
			taskData["deletedAt"] = now.Format(time.RFC3339)
			taskData["deleted"] = true
			taskData["status"] = "deleted"

			// Update the last error if this was due to max retries
			if taskData["retryCount"] != nil && taskData["maxRetries"] != nil {
				retryCount, _ := taskData["retryCount"].(float64)
				maxRetries, _ := taskData["maxRetries"].(float64)
				if retryCount >= maxRetries {
					taskData["lastError"] = fmt.Sprintf("Task reached max retries (%d/%d)", int(retryCount), int(maxRetries))
					taskData["lastErrorTime"] = now.Format(time.RFC3339)
				}
			}

			if updatedData, err := json.Marshal(taskData); err == nil {
				deletedTask.TaskData = string(updatedData)
			}
		}
	}

	// Marshal to JSON
	data, err := json.Marshal(&deletedTask)
	if err != nil {
		return fmt.Errorf("marshal task for deletion: %w", err)
	}

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

	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("sync file: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close file: %w", err)
	}

	// Update stats
	fs.appendCount++
	fs.deletedTasks++

	// Log the deletion
	fmt.Printf("Marked task %s as deleted at %s\n", taskID, now.Format(time.RFC3339))

	// Check if compaction should be triggered
	if fs.shouldCompact() {
		fmt.Println("Compacting task log file due to high deletion ratio...")
		go func() {
			if err := fs.Compact(); err != nil {
				fmt.Printf("Error compacting file: %v\n", err)
			} else {
				fmt.Println("Successfully compacted task log file")
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

	// Update stats.
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

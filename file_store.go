package snerd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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

	file, err := os.Open(fs.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// Return empty slice if file doesn't exist yet
			return []*RetryableTask{}, nil
		}
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Printf("Error closing file: %s\n", err)
			return
		}
	}(file)

	// Build map of latest tasks by ID
	taskMap := make(map[string]*RetryableTask)
	scanner := bufio.NewScanner(file)
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
// This filters out deleted tasks and returns only active tasks
func (fs *FileStore) ReadDueTasks() ([]*RetryableTask, error) {
	// Get all active tasks first
	tasks, err := fs.ReadTasks()
	if err != nil {
		return nil, fmt.Errorf("read tasks: %w", err)
	}
	
	// No additional filtering is needed here - we'll let the caller determine
	// which tasks are "due" based on their RetryAfterTime. We've already filtered
	// out deleted tasks in ReadTasks()
	return tasks, nil
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

	return fs.CreateTask(latest)

}

// We use a soft deleting approach alongside compaction
func (fs *FileStore) DeleteTask(taskID string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	now := time.Now()
	// Construct the soft delete marker task
	t := &RetryableTask{
		TaskID:    taskID,
		DeletedAt: &now, // pointer so nil vs non-nil matters
		// Optional: keep previous values or add metadata if needed
	}

	// Marshal to JSON
	data, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("marshal soft delete: %w", err)
	}

	// Open the file for appending
	f, err := os.OpenFile(fs.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	fmt.Println("Opened file for appending")
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			fmt.Printf("Error closing file: %s\n", err)
			return
		}
	}(f)

	// Append the JSON entry followed by a newline
	if _, err := f.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("write to file: %w", err)
	}

	// Track stats
	fs.appendCount++
	if t.DeletedAt != nil {
		fs.deletedTasks++
	} else {
		fs.totalTasks++
	}

	// Check if compaction should be triggered
	if fs.shouldCompact() {
		go func() {
			err := fs.Compact()
			if err != nil {
				fmt.Printf("Error compacting file: %s\n", err)
			}
		}()

	}

	return nil
}

// our implementation of Compaction. Compaction is the process of cleaning up the task log file by removing obsolete
// entries—such as soft-deleted or outdated task versions—to reduce file size and improve read efficiency.
func (fs *FileStore) Compact() error {
	fmt.Println("Compacting file")
	if !compacting.CompareAndSwap(false, true) {
		return nil // already compacting
	}
	defer compacting.Store(false)

	fs.mu.Lock()
	defer fs.mu.Unlock()

	fmt.Println("Opening file:", fs.filePath)
	f, err := os.Open(fs.filePath)
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

	fmt.Println("Building latest state of each task")
	// Build the latest state of each task
	taskMap := make(map[string]*RetryableTask)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		var t RetryableTask
		if err := json.Unmarshal(line, &t); err != nil {
			fmt.Printf("unmarshal task: %v\n", err)
			continue
		}

		if t.DeletedAt != nil {
			delete(taskMap, t.TaskID)
		} else {
			taskMap[t.TaskID] = &t
		}

	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan file: %w", err)
	}
	fmt.Println("Scanned file")
	//write to temp file
	tempPath := fs.filePath + ".tmp"
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	fmt.Println("Created temp file:", tempFile)
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
	if err := os.Rename(tempPath, fs.filePath); err != nil {
		return fmt.Errorf("rename tempfile: %w", err)
	}
	fmt.Println("Renamed tempfile to file")

	return nil

}

func (fs *FileStore) shouldCompact() bool {
	// NOTE: This method should only be called when the mutex is already held
	// by the caller, so we don't need to lock again here

	// File size threshold: > 20MB
	if info, err := os.Stat(fs.filePath); err == nil && info.Size() > 20*1024*1024 {
		return true
	}

	// Deleted ratio threshold: > 50%
	if fs.totalTasks > 0 && float64(fs.deletedTasks)/float64(fs.totalTasks) > 0.5 {
		return true
	}

	// Append count threshold: every 10,000 appends
	if fs.appendCount >= 10000 {
		fs.appendCount = 0 // reset counter
		return true
	}

	return false
}

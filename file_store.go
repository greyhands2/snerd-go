package snerd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
	// get the mutex lock and unlock out of the way
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Ensure parent directory exists before creating or appending to the file
	if err := os.MkdirAll(filepath.Dir(fs.filePath), 0755); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}
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

	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal task: %w", err)
	}

	_, err = f.Write(append(data, '\n'))
	if err != nil {
		return fmt.Errorf("write task: %w", err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync task: %w", err)
	}

	// Track stats
	fs.appendCount++
	if task.DeletedAt != nil {
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

func (fs *FileStore) ReadTasks() ([]*RetryableTask, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	f, err := os.Open(fs.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// No tasks file yet; return empty slice, not an error
			return []*RetryableTask{}, nil
		}
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer func(f *os.File) {
		if err := f.Close(); err != nil {
			fmt.Printf("Error closing file: %s\n", err)
		}
	}(f)

	// Map to keep latest version of each task by ID
	taskMap := make(map[string]*RetryableTask)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()

		var t RetryableTask
		if err := json.Unmarshal(line, &t); err != nil {
			fmt.Printf("unmarshal task: %v\n", err)
			continue
		}

		// Always overwrite — assuming each line is an event (append-only)
		taskMap[t.TaskID] = &t
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan file: %w", err)
	}

	// Filter out tasks that are soft-deleted in their latest version
	var tasks []*RetryableTask
	for _, t := range taskMap {
		if t.DeletedAt == nil {
			tasks = append(tasks, t)
		}
	}

	return tasks, nil
}

func (fs *FileStore) UpdateTaskRetryConfig(taskID string) error {
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
	latest.RetryAfterTime = time.Now().Add(time.Duration(latest.RetryAfter) * time.
		Hour)

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
	if !compacting.CompareAndSwap(false, true) {
		return nil // already compacting
	}
	defer compacting.Store(false)

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

	//write to temp file
	tempPath := fs.filePath + ".tmp"
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

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

	// finally atomically replace old file
	if err := os.Rename(tempPath, fs.filePath); err != nil {
		return fmt.Errorf("rename tempfile: %w", err)
	}

	return nil

}

func (fs *FileStore) shouldCompact() bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()

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

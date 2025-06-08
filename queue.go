package snerd

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// AnyQueue is a thread-safe queue that manages SnerdTask execution, retry logic, and statistics
type AnyQueue struct {
	name            string
	maxSize         int
	mu              sync.Mutex
	totalEnqueued   int64
	totalDequeued   int64
	fileStore       *FileStore
	processorActive bool
	processorCtx    context.Context
	processorCancel context.CancelFunc
}

// TaskFactory creates a Task from its stored data.
// The factory function is responsible for reconstructing a Task instance, including unmarshaling any stored data.
type TaskFactory func(id string, data string) (Task, error)

// NewAnyQueue creates a new queue with the given parameters
func NewAnyQueue(args ...interface{}) *AnyQueue {
	var name string = "default-queue"
	var maxSize int = 100                                    // reasonable default
	var taskStorePath string = "./.snerdata/tasks/tasks.log" // Use the hidden .snerdata folder
	var processingInterval time.Duration = 10 * time.Second  // Default processing interval

	// Try to determine the arguments based on their types
	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			name = v
		case int:
			maxSize = v
		case time.Duration:
			processingInterval = v
		}
	}

	// Create the queue with the specified parameters
	q := &AnyQueue{
		name:            name,
		maxSize:         maxSize,
		processorActive: false,
	}

	// Initialize the file store (uses .snerdata hidden folder)
	fileStore, err := NewFileStore(taskStorePath)
	if err != nil {
		fmt.Printf("Warning: Could not initialize file store: %v\n", err)
		// Create default empty file store path
		dirPath := filepath.Dir(taskStorePath)
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			fmt.Printf("Error creating directory: %v\n", err)
		}
		// On Unix-like systems, directories starting with a dot are already hidden
		// For Windows, hide the directory
		if runtime.GOOS == "windows" {
			// Get the parent directory to find the .snerdata folder
			snerDataDir := filepath.Join(filepath.Dir(dirPath), ".snerdata")
			// Use attrib command to set the hidden attribute
			cmd := exec.Command("attrib", "+h", snerDataDir)
			if err := cmd.Run(); err != nil {
				fmt.Printf("Warning: Could not hide directory on Windows: %v\n", err)
			}
		}
		// Try again with empty file
		fileStore, err = NewFileStore(taskStorePath)
		if err != nil {
			fmt.Printf("Error: Still could not initialize file store: %v\n", err)
		}
	}
	q.fileStore = fileStore

	// Start the task processor in the background
	q.startProcessor(processingInterval)

	return q
}

// startProcessor starts the background task processor
func (q *AnyQueue) startProcessor(interval time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.processorActive {
		return // Already running
	}

	q.processorCtx, q.processorCancel = context.WithCancel(context.Background())
	q.processorActive = true

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-q.processorCtx.Done():
				q.mu.Lock()
				q.processorActive = false
				q.mu.Unlock()
				return
			case <-ticker.C:
				q.ProcessDueTasks() // Process on each tick
			}
		}
	}()
}

// StopProcessor stops the background task processor
func (q *AnyQueue) StopProcessor() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if !q.processorActive || q.processorCancel == nil {
		return // Not running
	}

	q.processorCancel()
	q.processorActive = false
}

// Enqueue adds a task to the queue
func (q *AnyQueue) Enqueue(task Task) error {
	// Handle SnerdTask directly
	if snerdTask, ok := task.(*SnerdTask); ok {
		return q.EnqueueSnerdTask(snerdTask)
	}

	// For any non-SnerdTask, return an error as we no longer support legacy task types
	return fmt.Errorf("legacy task types are no longer supported; use SnerdTask instead")
}

// EnqueueSnerdTask adds a parameter-based SnerdTask to the queue for execution
// This is the preferred method for adding new tasks as it uses the parameter-based
// approach that doesn't require client-side task registration
func (q *AnyQueue) EnqueueSnerdTask(task *SnerdTask) error {
	// Convert the SnerdTask to a RetryableTask for storage
	zeroTime := time.Time{}
	task.DeletedAt = &zeroTime
	retryableTask := task.ToRetryableTask()

	// Store the task in the file store
	if q.fileStore != nil {
		if err := q.fileStore.CreateTask(retryableTask); err != nil {
			return fmt.Errorf("failed to store task: %w", err)
		}
	}

	// Update queue stats
	atomic.AddInt64(&q.totalEnqueued, 1)

	// Process immediately if the task is due
	if task.RetryAfterTime.Before(time.Now()) {
		fmt.Println("THE FIRST TASK WOULD CERTAINLY HAPPEN!!!!!!!")
		go func() {
			// Execute the task
			if err := task.Execute(); err != nil {
				// Update retry configuration if we haven't exceeded max retries
				fmt.Println("THERE WAS AN ERROR", task.RetryCount, task.MaxRetries)
				if task.RetryCount < task.MaxRetries {
					// Let the FileStore update the retry config and calculate the next retry time
					// The UpdateTaskRetryConfig method will handle:
					// 1. Incrementing retry count
					// 2. Calculating proper next retry time
					// 3. Storing the error information
					if q.fileStore != nil {
						if updateErr := q.fileStore.UpdateTaskRetryConfig(task.GetTaskID(), err); updateErr != nil {
							fmt.Printf("Error updating task retry config: %v\n", updateErr)
						} else {
							// Calculate and display when the next retry will happen
							task.RetryCount++ // Local update for logging only
							retryTime := time.Now().Add(time.Duration(task.RetryAfterHours * float64(time.Hour)))
							fmt.Printf("Task %s failed with error: %v\n", task.GetTaskID(), err)
							fmt.Printf("Scheduled for retry %d/%d at %s\n",
								task.RetryCount, task.MaxRetries, retryTime.Format(time.RFC3339))
						}
					} else {
						// No filestore available, log an error
						fmt.Printf("Warning: Cannot update task %s - no file store available\n", task.GetTaskID())
					}
				} else {
					// Task reached max retries
					if callbackErr := task.OnMaxRetryReached(nil); callbackErr != nil {
						fmt.Printf("Error executing OnMaxRetryReached: %v\n", callbackErr)
					}

					// Delete the task
					if q.fileStore != nil {
						if deleteErr := q.fileStore.DeleteTask(task.GetTaskID()); deleteErr != nil {
							fmt.Printf("Error deleting task: %v\n", deleteErr)
						}
					}
					atomic.AddInt64(&q.totalDequeued, 1)
				}
			} else {
				// Task executed successfully
				if q.fileStore != nil {
					if deleteErr := q.fileStore.DeleteTask(task.GetTaskID()); deleteErr != nil {
						fmt.Printf("Error deleting task: %v\n", deleteErr)
					}
				}
				atomic.AddInt64(&q.totalDequeued, 1)
			}
		}()
	}

	return nil
}

func (q *AnyQueue) processTask(task Task) {
	// Just execute the task directly
	if err := task.Execute(); err != nil {
		fmt.Printf("error executing task: %v\n", err)
	}
}

// ProcessDueTasks processes all tasks that are due for execution
func (q *AnyQueue) ProcessDueTasks() {
	// Step 1: Load all tasks from the FileStore
	if q.fileStore == nil {
		fmt.Println("No file store available for processing tasks")
		return
	}

	now := time.Now()
	fmt.Printf("[%s] Processing due tasks...\n", now.Format(time.RFC3339Nano))
	tasks, err := q.fileStore.ReadDueTasks()
	if err != nil {
		fmt.Printf("Error reading tasks: %v\n", err)
		return
	}

	// No need to filter here, ReadDueTasks already returns only due tasks
	if len(tasks) == 0 {
		fmt.Printf("[%s] No due tasks found at %s\n", now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
		return
	}

	fmt.Printf("[%s] Found %d due tasks (current time: %s)\n", now.Format(time.RFC3339Nano), len(tasks), now.Format(time.RFC3339Nano))
	
	// Sort tasks by RetryAfterTime (earliest first) to ensure fairness
	sort.Slice(tasks, func(i, j int) bool {
		// If either time is zero, prioritize it
		if tasks[i].RetryAfterTime.IsZero() {
			return true
		}
		if tasks[j].RetryAfterTime.IsZero() {
			return false
		}
		return tasks[i].RetryAfterTime.Before(tasks[j].RetryAfterTime)
	})

	// Step 2: Process each due task
	for _, t := range tasks {
		// Convert RetryableTask to SnerdTask for execution
		snerdTask := FromRetryableTask(t)

		// Skip tasks with missing type or parameters
		if snerdTask.TaskType == "" {
			fmt.Printf("Skipping task %s: missing task type\n", snerdTask.GetTaskID())
			continue
		}

		// If this is the first execution, set the initial RetryAfterTime
		if snerdTask.RetryAfterTime.IsZero() {
			snerdTask.RetryAfterTime = time.Now()
		}

		// Check if task type has a registered handler
		handlersMutex.RLock()
		handler, exists := taskHandlers[snerdTask.TaskType]
		handlersMutex.RUnlock()

		if !exists {
			fmt.Printf("No handler registered for task type: %s\n", snerdTask.TaskType)
			continue
		}

		// Log task execution
		fmt.Printf("Executing task %s (type: %s, attempt %d/%d)\n", 
			snerdTask.TaskID, snerdTask.TaskType, snerdTask.RetryCount+1, snerdTask.MaxRetries+1)

		// Execute the task
		err := handler(snerdTask.Parameters)
		if err != nil {
			// Log the error
			fmt.Printf("Task %s failed: %v\n", snerdTask.TaskID, err)
			
			// Update retry configuration
			snerdTask.UpdateRetryConfig(err)

			// If we've exceeded max retries, mark as failed
			if snerdTask.RetryCount >= snerdTask.MaxRetries {
				fmt.Printf("Task %s failed after %d retries: %v\n", snerdTask.TaskID, snerdTask.MaxRetries, err)
				// Mark task as failed in the file store
				if q.fileStore != nil {
					_ = q.fileStore.DeleteTask(snerdTask.TaskID)
				}
			} else {
				// Calculate next retry time with exponential backoff
				retryHours := snerdTask.RetryAfterHours
				if retryHours <= 0 {
					// Default to 1 minute if not specified
					retryHours = 1.0 / 60.0
				}

				// Apply exponential backoff
				backoffFactor := math.Pow(2, float64(snerdTask.RetryCount))
				retryDuration := time.Duration(retryHours*backoffFactor*float64(time.Hour)) * time.Second

				// Log the retry information
				fmt.Printf("Scheduling task %s for retry %d/%d after %v\n",
					snerdTask.TaskID,
					snerdTask.RetryCount+1,
					snerdTask.MaxRetries,
					retryDuration.Round(time.Second))

				// Update the task for retry
				if q.fileStore != nil {
					updateErr := q.fileStore.UpdateTaskRetryConfig(snerdTask.TaskID, err)
					if updateErr != nil {
						fmt.Printf("Error updating task retry config: %v\n", updateErr)
					} else {
						fmt.Printf("Successfully updated task %s for retry %d/%d\n", 
							snerdTask.TaskID, snerdTask.RetryCount+1, snerdTask.MaxRetries)
					}
				} else {
					fmt.Printf("Warning: Cannot update task %s - no file store available\n", snerdTask.TaskID)
				}
			}
			// Log the final error after max retries
			fmt.Printf("Task %s failed after %d attempts: %v\n", snerdTask.TaskID, snerdTask.MaxRetries, err)

			// Delete the task after it has reached max retries
			if q.fileStore != nil {
				deleteErr := q.fileStore.DeleteTask(snerdTask.TaskID)
				if deleteErr != nil {
					fmt.Printf("Error deleting task: %v\n", deleteErr)
				} else {
					fmt.Printf("Successfully deleted task %s after max retries\n", snerdTask.TaskID)
				}
			}
		} else {
			// Task executed successfully
			fmt.Printf("Task %s executed successfully\n", snerdTask.GetTaskID())

			// Delete the task after successful execution
			if q.fileStore != nil {
				// Ensure we soft-delete by using the proper method
				deleteErr := q.fileStore.DeleteTask(snerdTask.GetTaskID())
				if deleteErr != nil {
					fmt.Printf("Error deleting task %s: %v\n", snerdTask.GetTaskID(), deleteErr)
				} else {
					fmt.Printf("Successfully deleted task %s after completion\n", snerdTask.GetTaskID())

					// Record task completion statistics
					duration := time.Since(snerdTask.CreatedAt)
					fmt.Printf("Task %s completed in %v (type=%s)\n",
						snerdTask.GetTaskID(),
						duration.Round(time.Millisecond),
						snerdTask.TaskType)
				}
			}
		}
		atomic.AddInt64(&q.totalDequeued, 1)
	}
}
func (q *AnyQueue) Name() string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.name
}

// Size returns the number of active tasks currently in the queue.
func (q *AnyQueue) Size() int {
	// With the new implementation, we need to count tasks from the FileStore
	if q.fileStore == nil {
		return 0
	}

	tasks, err := q.fileStore.ReadDueTasks()
	if err != nil {
		fmt.Printf("Error reading tasks: %v\n", err)
		return 0
	}

	return len(tasks)
}

// RemainingCapacity returns the number of additional tasks that can be enqueued before reaching maxSize..
func (q *AnyQueue) RemainingCapacity() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.maxSize - q.Size()
}

// TotalProcessed returns the total number of tasks that have been processed and dequeued.
func (q *AnyQueue) TotalProcessed() int {
	return int(atomic.LoadInt64(&q.totalDequeued)) // atomic read
}

// TotalEnqueued returns the total number of tasks that have been enqueued.
func (q *AnyQueue) TotalEnqueued() int {
	return int(atomic.LoadInt64(&q.totalEnqueued)) // atomic read
}

// TaskWithData extends Task to support saving and retrieving task-specific data.
// Implement this interface if your task needs to persist additional fields.
type TaskWithData interface {
	Task
	// GetTaskType returns a unique identifier for this task type.
	// This is used for debugging and monitoring, not for type-based dispatch.
	GetTaskType() string
	// MarshalData serializes the task data to JSON.
	MarshalData() ([]byte, error)
	// UnmarshalData deserializes the task data from JSON.
	UnmarshalData([]byte) error
	// Clone creates a new instance of this task with the same type but no data.
	// This will be populated via UnmarshalData when reconstructing tasks.
	Clone() TaskWithData
}

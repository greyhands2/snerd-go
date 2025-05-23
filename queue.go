package snerd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// AnyQueue is a thread-safe queue that supports both in-memory and retryable (persistent) tasks.
// It manages task execution, retry logic, and queue statistics.
type AnyQueue struct {
	name            string
	maxSize         int
	mu              sync.Mutex
	activeTasks     map[string]Task // In-memory queue
	totalEnqueued   int64
	totalDequeued   int64
	fileStore       *FileStore
	processorActive bool
	processorCtx    context.Context
	processorCancel context.CancelFunc
}

// NewAnyQueue creates a new queue with the given parameters
// This constructor is highly flexible for backward compatibility and can handle any existing call patterns
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
		activeTasks:     make(map[string]Task),
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

// Enqueue adds a Task to the queue for processing.
// This method now supports both traditional Task objects and the new SnerdTask objects
// for a simpler parameter-based API.
func (q *AnyQueue) Enqueue(task Task) error {
	// Handle SnerdTask specially with the optimized method
	if snerdTask, ok := task.(*SnerdTask); ok {
		return q.EnqueueSnerdTask(snerdTask)
	}
	
	atomic.AddInt64(&q.totalEnqueued, 1)
	
	q.mu.Lock()
	defer q.mu.Unlock()

	// Check if this is a retryable task or in-memory task
	_, isRetryable := task.(interface {
		GetMaxRetries() int
		GetRetryAfterHours() float64
	})

	// For non-retryable memory-only tasks
	if !isRetryable {
		// This is a simple in-memory task with no retries
		if len(q.activeTasks) >= q.maxSize {
			return errors.New("queue is full: rejecting new task")
		}
		q.activeTasks[task.GetTaskID()] = task
		go q.processTask(task)
		return nil
	}
	fmt.Println("Enqueueing retryable task:", task.GetTaskID())
	// For retryable tasks, store in the file store with task-specific retry settings
	retryTask := RetryableTask{
		TaskID:       task.GetTaskID(),
		RetryCount:   task.GetRetryCount(),
		EmbeddedTask: task, // Store the actual task object for later execution
	}

	// Get retry settings from task
	if taskWithRetry, ok := task.(interface {
		GetMaxRetries() int
		GetRetryAfterHours() float64
	}); ok {
		retryTask.MaxRetries = taskWithRetry.GetMaxRetries()
		retryTask.RetryAfterHours = taskWithRetry.GetRetryAfterHours()
	} else {
		// Default values if task doesn't provide them
		retryTask.MaxRetries = 5
		retryTask.RetryAfterHours = 1.0
	}

	retryTask.RetryAfterTime = time.Now().Add(time.Duration(retryTask.RetryAfterHours) * time.Hour)
	
	// Get task type for registry lookup
	if taskWithType, ok := task.(interface{ GetTaskType() string }); ok {
		retryTask.TaskType = taskWithType.GetTaskType()
		fmt.Println("Setting task type to:", retryTask.TaskType)
	} else {
		fmt.Println("WARNING: Task doesn't implement GetTaskType() - using any-task as fallback")
		retryTask.TaskType = "any-task"
	}

	// Store minimal task data for diagnostic/debugging purposes
	if data, err := json.Marshal(map[string]string{
		"type": retryTask.TaskType,
		"id":   retryTask.TaskID,
	}); err == nil {
		retryTask.TaskData = string(data)
	}

	// Store the task using the file store
	if q.fileStore != nil {
		err := q.fileStore.CreateTask(&retryTask)
		if err != nil {
			return fmt.Errorf("failed to store task: %w", err)
		}
		fmt.Println("Enqueued task:", retryTask)
		return nil
	} else {
		// Fallback to the old Save method for backward compatibility
		err := retryTask.Save()
		if err != nil {
			return fmt.Errorf("failed to save task: %w", err)
		}
		fmt.Println("Enqueued task (legacy method):", retryTask)
		return nil
	}
}

// EnqueueSnerdTask adds a parameter-based SnerdTask to the queue for execution
// This is the preferred method for adding new tasks as it uses the parameter-based
// approach that doesn't require client-side task registration
func (q *AnyQueue) EnqueueSnerdTask(task *SnerdTask) error {
	// Convert the SnerdTask to a RetryableTask for storage
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
		go func() {
			// Execute the task
			if err := task.Execute(); err != nil {
				// Update retry configuration if we haven't exceeded max retries
				if task.RetryCount < task.MaxRetries {
					task.UpdateRetryConfig(err)

					// Save the updated task
					retryableTask = task.ToRetryableTask()
					if q.fileStore != nil {
						if updateErr := q.fileStore.CreateTask(retryableTask); updateErr != nil {
							fmt.Printf("Error updating task: %v\n", updateErr)
						}
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
	err := task.Execute()
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.activeTasks, task.GetTaskID()) // Remove from memory
	if err != nil {
		fmt.Println("Task failed but has no retries:", task.GetTaskID())
	} else {
		atomic.AddInt64(&q.totalDequeued, 1)
	}
}

// ProcessDueTasks processes all tasks that are due for execution (retry time has passed).
func (q *AnyQueue) ProcessDueTasks() {
	if q.fileStore == nil {
		fmt.Println("Error: File store not initialized")
		return
	}

	fmt.Println("Processing due tasks...")

	// Step 1: Read all tasks from storage
	tasks, err := q.fileStore.ReadTasks()
	if err != nil {
		fmt.Printf("Error reading tasks: %v\n", err)
		return
	}

	// Filter tasks that are due for execution (retry time has passed)
	var dueTasks []*RetryableTask
	for _, t := range tasks {
		if t.RetryAfterTime.Before(time.Now()) {
			dueTasks = append(dueTasks, t)
		}
	}

	fmt.Printf("Found %d due tasks\n", len(dueTasks))

	// Step 2: Process each due task
	for _, t := range dueTasks {
		// Handle both legacy RetryableTask and new SnerdTask types
		if snerdTask, ok := t.EmbeddedTask.(*SnerdTask); ok {
			// Process SnerdTask using the new approach
			fmt.Printf("Executing SnerdTask %s (type=%s)\n", snerdTask.GetTaskID(), snerdTask.TaskType)

			// Execute the task using the registered handler
			err := snerdTask.Execute()
			if err != nil {
				fmt.Printf("Error executing task %s: %v\n", snerdTask.GetTaskID(), err)

				// Update retry configuration if we haven't exceeded max retries
				if snerdTask.RetryCount < snerdTask.MaxRetries {
					// Update retry configuration
					snerdTask.UpdateRetryConfig(err)

					// Save the updated task
					if updateErr := q.fileStore.CreateTask(t); updateErr != nil {
						fmt.Printf("Error updating task in storage: %v\n", updateErr)
					} else {
						fmt.Printf("Successfully updated task %s for retry\n", snerdTask.GetTaskID())
					}
				} else {
					// Task reached max retries, handle it
					fmt.Printf("Task %s reached max retries (%d)\n", snerdTask.GetTaskID(), snerdTask.MaxRetries)

					// Execute OnMaxRetryReached handler if implemented
					if callbackErr := snerdTask.OnMaxRetryReached(nil); callbackErr != nil {
						fmt.Printf("Error executing OnMaxRetryReached: %v\n", callbackErr)
					}

					// Delete the task
					if deleteErr := q.fileStore.DeleteTask(snerdTask.GetTaskID()); deleteErr != nil {
						fmt.Printf("Error deleting task: %v\n", deleteErr)
					} else {
						fmt.Printf("Successfully deleted task %s\n", snerdTask.GetTaskID())
						atomic.AddInt64(&q.totalDequeued, 1)
					}
				}
			} else {
				// Task executed successfully, remove it
				fmt.Printf("Task %s executed successfully\n", snerdTask.GetTaskID())

				// Delete the task
				if deleteErr := q.fileStore.DeleteTask(snerdTask.GetTaskID()); deleteErr != nil {
					fmt.Printf("Error deleting task: %v\n", deleteErr)
				} else {
					fmt.Printf("Successfully deleted task %s\n", snerdTask.GetTaskID())
					atomic.AddInt64(&q.totalDequeued, 1)
				}
			}
		} else {
			// Legacy RetryableTask handling (for backward compatibility)
			fmt.Printf("Executing legacy task %s (type=%s)\n", t.GetTaskID(), t.TaskType)

			// Execute the task
			err := t.Execute()
			if err != nil {
				fmt.Printf("Error executing task %s: %v\n", t.GetTaskID(), err)

				// Handle retry logic for legacy tasks
				if t.RetryCount < t.MaxRetries {
					// Increment retry count and reschedule
					t.RetryCount++
					t.RetryAfterTime = time.Now().Add(time.Duration(t.RetryAfterHours * float64(time.Hour)))

					// Save the updated task
					if updateErr := q.fileStore.CreateTask(t); updateErr != nil {
						fmt.Printf("Error updating legacy task: %v\n", updateErr)
					}
				} else {
					// Legacy task reached max retries
					fmt.Printf("Legacy task %s reached max retries\n", t.GetTaskID())

					// Handle max retry callback if implemented
					if maxRetryHandler, ok := t.EmbeddedTask.(TaskWithMaxRetryCallback); ok && maxRetryHandler != nil {
						if callbackErr := maxRetryHandler.OnMaxRetryReached(nil); callbackErr != nil {
							fmt.Printf("Error executing legacy OnMaxRetryReached: %v\n", callbackErr)
						}
					}

					// Delete the task
					if deleteErr := q.fileStore.DeleteTask(t.GetTaskID()); deleteErr != nil {
						fmt.Printf("Error deleting legacy task: %v\n", deleteErr)
					}
				}
			} else {
				// Legacy task executed successfully
				fmt.Printf("Legacy task %s executed successfully\n", t.GetTaskID())

				// Delete the task
				if deleteErr := q.fileStore.DeleteTask(t.GetTaskID()); deleteErr != nil {
					fmt.Printf("Error deleting legacy task: %v\n", deleteErr)
				} else {
					atomic.AddInt64(&q.totalDequeued, 1)
				}
			}
		}
	}
}

// TaskFactory creates a Task from its stored data.
// The factory function is responsible for reconstructing a Task instance, including unmarshaling any stored data.
type TaskFactory func(id string, data string) (Task, error)

// Name returns the name of the queue.
func (q *AnyQueue) Name() string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.name
}

// Size returns the number of active tasks currently in the queue.
func (q *AnyQueue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.activeTasks)
}

// RemainingCapacity returns the number of additional tasks that can be enqueued before reaching maxSize.
func (q *AnyQueue) RemainingCapacity() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.maxSize - len(q.activeTasks)
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

package snerd

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// AnyQueue is a thread-safe queue that supports both in-memory and retryable (persistent) tasks.
// It manages task execution, retry logic, and queue statistics.
type AnyQueue struct {
	name          string
	maxSize       int
	mu            sync.Mutex
	activeTasks   map[string]Task // In-memory queue
	totalEnqueued int64
	totalDequeued int64
}

// NewAnyQueue creates a new queue with the given parameters
// This constructor is highly flexible for backward compatibility and can handle any existing call patterns
func NewAnyQueue(args ...interface{}) *AnyQueue {
	var name string = "default-queue"

	var maxSize int = 100 // reasonable default

	// Try to determine the arguments based on their types
	for _, arg := range args {
		switch v := arg.(type) {
		case string:
			name = v

		case int:
			maxSize = v
		}
	}

	return &AnyQueue{
		name:        name,
		maxSize:     maxSize,
		activeTasks: make(map[string]Task),
	}
}

// Enqueue adds a Task to the queue for processing.
// For in-memory tasks, it immediately starts execution in a goroutine.
// For retryable tasks, it persists the task and manages retry logic.
func (q *AnyQueue) Enqueue(task Task) error {
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
	// For retryable tasks, store in DB with task-specific retry settings
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
	fmt.Println("Retryable task settings:", retryTask)
	// Get task type for registry lookup
	if taskWithType, ok := task.(interface{ GetTaskType() string }); ok {
		retryTask.TaskType = taskWithType.GetTaskType()
		fmt.Println("Setting task type to:", retryTask.TaskType)
	} else {
		fmt.Println("WARNING: Task doesn't implement GetTaskType() - using any-task as fallback")
		retryTask.TaskType = "any-task"
	}

	// Get task type for diagnostic purposes only
	if taskWithType, ok := task.(interface{ GetTaskType() string }); ok {
		retryTask.TaskType = taskWithType.GetTaskType()
		fmt.Println("Setting task type to:", retryTask.TaskType)
	} else {
		retryTask.TaskType = "any-task"
	}

	// Store minimal task data for diagnostic/debugging purposes
	// The actual execution will use the embedded task directly
	if data, err := json.Marshal(map[string]string{
		"type": retryTask.TaskType,
		"id":   retryTask.TaskID,
	}); err == nil {
		retryTask.TaskData = string(data)
	}
	fmt.Println("Retryable task data:", retryTask)
	// DO NOT override task type - use the one from the task
	// Save to database
	err := retryTask.Save()
	if err == nil {
		fmt.Println("Enqueued task:", retryTask)
		atomic.AddInt64(&q.totalEnqueued, 1)
	}
	fmt.Println("Enqueued task:", retryTask)
	return err
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

// ProcessDueTasks processes all tasks that are due for execution.
// It uses the embedded task's execute method directly.
// This method is typically called periodically, either by a ticker or a scheduler.
func (q *AnyQueue) ProcessDueTasks() {
	fmt.Println("Processing due tasks for queue:", q.name)

	// Fetch tasks due for execution
	retryTasks, err := FetchDueTasks()
	if err != nil {
		fmt.Printf("Error fetching due tasks: %v\n", err)
		return
	}

	if len(retryTasks) > 0 {
		fmt.Printf("Found %d tasks to process\n", len(retryTasks))
	}

	for _, t := range retryTasks {
		// Debug info
		fmt.Printf("Executing task %s (type=%s)\n", t.TaskID, t.TaskType)

		// Create a wrapper task that will be executed
		taskWrapper := &RetryableTask{
			TaskID:          t.TaskID,
			RetryCount:      t.RetryCount,
			MaxRetries:      t.MaxRetries,
			RetryAfterHours: t.RetryAfterHours,
			RetryAfterTime:  t.RetryAfterTime,
			TaskData:        t.TaskData,
			TaskType:        t.TaskType,
			EmbeddedTask:    t.EmbeddedTask,
		}

		// Simple, direct task execution
		var task Task = taskWrapper.EmbeddedTask

		fmt.Printf("Executing task %s (type=%s) with embedded task %v\n", t.TaskID, t.TaskType, task)

		// Execute the task
		err := taskWrapper.Execute()
		if err != nil {
			fmt.Printf("Error executing task %s: %v\n", t.TaskID, err)
			// Increment retry count and reschedule if we haven't exceeded max retries
			if t.RetryCount < t.MaxRetries {

				// update to the database directly through Save method
				if updateErr := t.UpdateTaskRetryConfig(t.TaskID); updateErr != nil {
					fmt.Printf("Error updating retry task in database: %v\n", updateErr)
				} else {
					fmt.Printf("Successfully updated task %s in database\n", t.TaskID)
				}
			} else {
				// Remove task if max retries exceeded
				fmt.Printf("Task %s exceeded max retries (%d) - deleting\n", t.TaskID, t.MaxRetries)

				// Check if the task implements the OnMaxRetryReached callback
				if maxRetryHandler, ok := task.(TaskWithMaxRetryCallback); ok {
					fmt.Printf("Task %s implements OnMaxRetryReached, executing callback\n", t.TaskID)

					// Execute the callback
					if callbackErr := maxRetryHandler.OnMaxRetryReached(nil); callbackErr != nil {
						fmt.Printf("Error executing OnMaxRetryReached for task %s: %v\n", t.TaskID, callbackErr)
					} else {
						fmt.Printf("Successfully executed OnMaxRetryReached for task %s\n", t.TaskID)
					}
				}

				if deleteErr := DeleteTask(t.TaskID); deleteErr != nil {
					fmt.Printf("Error deleting retry task: %v\n", deleteErr)
				} else {
					fmt.Printf("Successfully deleted task %s from database\n", t.TaskID)
				}
			}
		} else {
			// Task executed successfully, remove it from the queue
			fmt.Printf("Task %s executed successfully - deleting\n", t.TaskID)
			if deleteErr := DeleteTask(t.TaskID); deleteErr != nil {
				fmt.Printf("Error deleting retry task: %v\n", deleteErr)
			} else {
				fmt.Printf("Successfully deleted task %s from database\n", t.TaskID)
			}
			atomic.AddInt64(&q.totalDequeued, 1)
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

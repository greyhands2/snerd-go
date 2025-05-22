package snerd

import (
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
		GetRetryAfterTime() time.Time
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

	// For retryable tasks, store in DB with task-specific retry settings
	retryTask := RetryableTask{
		TaskID:     task.GetTaskID(),
		RetryCount: task.GetRetryCount(),
	}

	// Get retry settings from task
	if taskWithRetry, ok := task.(interface {
		GetMaxRetries() int
		GetRetryAfterTime() time.Time
	}); ok {
		retryTask.MaxRetries = taskWithRetry.GetMaxRetries()
		retryTask.RetryAfterTime = taskWithRetry.GetRetryAfterTime()
	} else {
		// Default values if task doesn't provide them
		retryTask.MaxRetries = 5
		retryTask.RetryAfterTime = time.Now().Add(time.Duration(retryTask.RetryAfterHours) * time.Hour)
	}

	// Get task type for registry lookup
	if taskWithType, ok := task.(interface{ GetTaskType() string }); ok {
		retryTask.TaskType = taskWithType.GetTaskType()
	}

	// If task implements TaskWithData, serialize its data
	if taskWithData, ok := task.(TaskWithData); ok {
		data, err := taskWithData.MarshalData()
		if err != nil {
			return fmt.Errorf("failed to marshal task data: %w", err)
		}
		retryTask.TaskData = string(data)
	}

	// Save to database
	err := retryTask.Save()
	if err == nil {
		atomic.AddInt64(&q.totalEnqueued, 1)
	}
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
// It uses registered task factories to reconstruct and execute retryable tasks.
// This method is typically called periodically, either by a ticker or a scheduler.
func (q *AnyQueue) ProcessDueTasks(customFactory ...TaskFactory) {
	// Fetch tasks due for execution
	tasks, err := FetchDueTasks()
	if err != nil {
		fmt.Println("Error fetching retry tasks:", err)
		return
	}

	fmt.Println("THE TASKS LENGTH IS", len(tasks))

	for _, t := range tasks {
		var task Task
		var factoryErr error

		// Try to find a task factory based on task type
		// First try the custom factory if provided
		if len(customFactory) > 0 && customFactory[0] != nil {
			task, factoryErr = customFactory[0](t.TaskID, t.TaskData)
		} else {
			//
			// Try to find a registered factory
			factory, found := GetRegisteredTaskFactory(t)
			if !found {
				fmt.Printf("No factory found for task type: %s\n", t.TaskType)
				continue
			}
			task, factoryErr = factory(t.TaskID, t.TaskData)
		}

		if factoryErr != nil {
			fmt.Printf("Error creating task instance for %s: %v\n", t.TaskID, factoryErr)
			continue
		}

		fmt.Printf("Executing task %s (type=%s)\n", t.TaskID, t.TaskType)

		// Execute the task
		err := task.Execute()
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

					// Create a context provider function that checks for stored context data
					contextProvider := func() interface{} {
						// Check if the task stores its context data
						if contextHolder, ok := task.(interface{ GetRetryContext() interface{} }); ok {
							return contextHolder.GetRetryContext()
						}
						// Fallback to returning nil
						return nil
					}

					// Execute the callback with our context provider
					if callbackErr := maxRetryHandler.OnMaxRetryReached(contextProvider); callbackErr != nil {
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
	// MarshalData serializes the task data to JSON.
	MarshalData() ([]byte, error)
	// UnmarshalData deserializes the task data from JSON.
	UnmarshalData([]byte) error
}

package snerd

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Task represents a unit of work that can be processed by the queue system.
type Task interface {
	// GetTaskID returns the unique identifier for the task.
	GetTaskID() string
	// GetRetryCount returns the number of times this task has been retried.
	GetRetryCount() int
	// Execute runs the task's logic. Return an error if the task fails and should be retried.
	Execute() error
}

// TaskWithMaxRetryCallback allows a task to handle the case where it has reached its maximum number of retries.
type TaskWithMaxRetryCallback interface {
	// OnMaxRetryReached is called when the task reaches its maximum retry count.
	OnMaxRetryReached(contextProvider func() interface{}) error
}

// TaskHandler is a function that processes parameters to execute a task
type TaskHandler func(parameters string) error

// OnMaxRetryHandler is a function that handles when a task reaches max retries
type OnMaxRetryHandler func(parameters string) error

// Global handler registry with mutex for thread safety
var (
	taskHandlers       = make(map[string]TaskHandler)
	maxRetryHandlers   = make(map[string]OnMaxRetryHandler)
	handlersMutex      = &sync.RWMutex{}
)

// RegisterTaskHandler registers a handler for a specific task type
func RegisterTaskHandler(taskType string, handler TaskHandler) {
	handlersMutex.Lock()
	defer handlersMutex.Unlock()
	taskHandlers[taskType] = handler
}

// RegisterMaxRetryHandler registers a handler for when a task reaches max retries
func RegisterMaxRetryHandler(taskType string, handler OnMaxRetryHandler) {
	handlersMutex.Lock()
	defer handlersMutex.Unlock()
	maxRetryHandlers[taskType] = handler
}

// JobErrorReturn contains error information from task execution
type JobErrorReturn struct {
	ErrorObj    error
	RetryWorthy bool
}

// SnerdTask is a retryable task that stores parameters instead of implementations
type SnerdTask struct {
	// Core Task Identification
	TaskID          string    `json:"taskId"`           // Unique identifier for the task
	
	// Retry Configuration
	RetryCount      int       `json:"retryCount"`       // Current retry count
	MaxRetries      int       `json:"maxRetries"`       // Maximum number of retries allowed
	RetryAfterHours float64   `json:"retryAfterHours"`  // Hours to wait before retrying
	RetryAfterTime  time.Time `json:"retryAfterTime"`   // Timestamp for next retry attempt
	
	// Task Execution Data
	TaskType        string    `json:"taskType"`         // Type of task (maps to registered handler)
	Parameters      string    `json:"parameters"`       // JSON-encoded parameters for the task
	
	// Error Tracking
	LastErrorObj    error     `json:"lastErrorObj"`     // Last error that occurred
	LastJobError    *JobErrorReturn `json:"lastJobError"` // Detailed error information
	
	// Timestamps for record-keeping
	CreatedAt       time.Time  `json:"-"`               // When the task was created
	UpdatedAt       time.Time  `json:"-"`               // When the task was last updated
	DeletedAt       *time.Time `json:"-,omitempty"`     // Soft deletion timestamp
}

// NewSnerdTask creates a new task with the specified parameters
func NewSnerdTask(
	taskID string, 
	taskType string,
	parameters interface{},
	maxRetries int,
	retryAfterHours float64,
) (*SnerdTask, error) {
	// Serialize the parameters to JSON
	paramJSON, err := json.Marshal(parameters)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize parameters: %w", err)
	}
	
	return &SnerdTask{
		TaskID:          taskID,
		TaskType:        taskType,
		Parameters:      string(paramJSON),
		RetryCount:      0,
		MaxRetries:      maxRetries,
		RetryAfterHours: retryAfterHours,
		RetryAfterTime:  time.Now().Add(time.Duration(retryAfterHours * float64(time.Hour))),
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
	}, nil
}

// GetTaskID returns the task ID
func (t *SnerdTask) GetTaskID() string {
	return t.TaskID
}

// GetRetryCount returns the current retry count
func (t *SnerdTask) GetRetryCount() int {
	return t.RetryCount
}

// GetMaxRetries returns the maximum retry count
func (t *SnerdTask) GetMaxRetries() int {
	return t.MaxRetries
}

// GetRetryAfterTime returns the time when the task should be retried
func (t *SnerdTask) GetRetryAfterTime() time.Time {
	return t.RetryAfterTime
}

// GetRetryAfterHours returns the retry interval in hours
func (t *SnerdTask) GetRetryAfterHours() float64 {
	return t.RetryAfterHours
}

// Execute runs the task by invoking the registered handler
func (t *SnerdTask) Execute() error {
	handlersMutex.RLock()
	handler, exists := taskHandlers[t.TaskType]
	handlersMutex.RUnlock()
	
	if !exists || handler == nil {
		return fmt.Errorf("no handler registered for task type: %s", t.TaskType)
	}
	
	// Execute the handler with the task parameters
	return handler(t.Parameters)
}

// OnMaxRetryReached is called when the task reaches its maximum retry count
func (t *SnerdTask) OnMaxRetryReached(contextProvider func() interface{}) error {
	handlersMutex.RLock()
	handler, exists := maxRetryHandlers[t.TaskType]
	handlersMutex.RUnlock()
	
	if !exists || handler == nil {
		// Default behavior if no handler is registered
		fmt.Printf("Task %s (type=%s) reached max retries with no handler\n", t.TaskID, t.TaskType)
		return nil
	}
	
	// Execute the max retry handler
	return handler(t.Parameters)
}

// UpdateRetryConfig updates the retry configuration after a failed execution
func (t *SnerdTask) UpdateRetryConfig(errorObj error) {
	t.RetryCount++
	t.RetryAfterTime = time.Now().Add(time.Duration(t.RetryAfterHours * float64(time.Hour)))
	t.LastErrorObj = errorObj
	t.UpdatedAt = time.Now()
}

// ToRetryableTask wraps the SnerdTask in a RetryableTask for storage compatibility
func (t *SnerdTask) ToRetryableTask() *RetryableTask {
	// Convert parameters to JSON string
	return &RetryableTask{
		TaskID:          t.TaskID,
		RetryCount:      t.RetryCount,
		MaxRetries:      t.MaxRetries,
		RetryAfterHours: t.RetryAfterHours,
		RetryAfterTime:  t.RetryAfterTime,
		TaskData:        t.Parameters,
		TaskType:        t.TaskType,
		EmbeddedTask:    t,
		DeletedAt:       t.DeletedAt,
		CreatedAt:       t.CreatedAt,
		UpdatedAt:       t.UpdatedAt,
	}
}

// CreateTask is a convenience function that creates a new task with the given parameters
// This is the simplified client API function for creating parameter-based tasks
func CreateTask(taskID string, taskType string, parameters interface{}, maxRetries int, retryAfterHours float64) (*SnerdTask, error) {
	return NewSnerdTask(taskID, taskType, parameters, maxRetries, retryAfterHours)
}


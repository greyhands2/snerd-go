package snerd

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
	taskHandlers     = make(map[string]TaskHandler)
	maxRetryHandlers = make(map[string]OnMaxRetryHandler)
	handlersMutex    = &sync.RWMutex{}
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
// JobErrorReturn holds error information for a failed job
// and implements custom JSON marshaling/unmarshaling to handle the error type
type JobErrorReturn struct {
	ErrorObj    error
	ErrorString string `json:"error"` // Used for JSON serialization
	RetryWorthy bool   `json:"retry_worthy"`
}

// MarshalJSON implements json.Marshaler interface
func (j JobErrorReturn) MarshalJSON() ([]byte, error) {
	// Create a struct that can be safely marshaled
	temp := struct {
		ErrorString string `json:"error"`
		RetryWorthy bool   `json:"retry_worthy"`
	}{
		ErrorString: "",
		RetryWorthy: j.RetryWorthy,
	}

	// Convert error to string if not nil
	if j.ErrorObj != nil {
		temp.ErrorString = j.ErrorObj.Error()
	}

	return json.Marshal(temp)
}

// UnmarshalJSON implements json.Unmarshaler interface
func (j *JobErrorReturn) UnmarshalJSON(data []byte) error {
	// Create a temporary struct to unmarshal into
	temp := struct {
		ErrorString string `json:"error"`
		RetryWorthy bool   `json:"retry_worthy"`
	}{}

	// Unmarshal the JSON into our temporary struct
	if err := json.Unmarshal(data, &temp); err != nil {
		return fmt.Errorf("failed to unmarshal JobErrorReturn: %w", err)
	}

	// Set the fields on our actual struct
	if temp.ErrorString != "" {
		j.ErrorObj = errors.New(temp.ErrorString)
	}
	j.ErrorString = temp.ErrorString
	j.RetryWorthy = temp.RetryWorthy

	return nil
}

// SnerdTask is a retryable task that stores parameters instead of implementations
type SnerdTask struct {
	// Core Task Identification
	TaskID string `json:"taskId"` // Unique identifier for the task

	// Retry Configuration
	RetryCount      int       `json:"retryCount"`      // Current retry count
	MaxRetries      int       `json:"maxRetries"`      // Maximum number of retries allowed
	RetryAfterHours float64   `json:"retryAfterHours"` // Hours to wait before retrying
	RetryAfterTime  time.Time `json:"retryAfterTime"`  // Timestamp for next retry attempt

	// Task Execution Data
	TaskType   string `json:"taskType"`   // Type of task (maps to registered handler)
	Parameters string `json:"parameters"` // JSON-encoded parameters for the task

	// Error Tracking
	LastErrorObj error           `json:"lastErrorObj"` // Last error that occurred
	LastJobError *JobErrorReturn `json:"lastJobError"` // Detailed error information

	// Timestamps for record-keeping
	CreatedAt time.Time  `json:"-"`                   // When the task was created
	UpdatedAt time.Time  `json:"-"`                   // When the task was last updated
	DeletedAt *time.Time `json:"deletedAt,omitempty"` // Soft deletion timestamp
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

	// For new tasks, set RetryAfterTime to now (immediately due)
	// Only tasks being retried will have RetryAfterTime set to the future
	return &SnerdTask{
		TaskID:          taskID,
		TaskType:        taskType,
		Parameters:      string(paramJSON),
		RetryCount:      0,
		MaxRetries:      maxRetries,
		RetryAfterHours: retryAfterHours,
		RetryAfterTime:  time.Now(), // Make new tasks immediately due
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
	}, nil
}

// GetTaskID returns the task ID
func (t *SnerdTask) GetTaskID() string {
	return t.TaskID
}
func (t *SnerdTask) String() string {
	return fmt.Sprintf("RetryableTask{TaskID: %s, RetryCount: %d, TaskType: %s}",
		t.TaskID, t.RetryCount, t.TaskType)
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
	// Log execution for debugging
	fmt.Printf("Executing SnerdTask: ID=%s, Type=%s\n", t.TaskID, t.TaskType)

	// Get the handler for this task type
	handlersMutex.RLock()
	handler, exists := taskHandlers[t.TaskType]
	handlersMutex.RUnlock()

	if !exists || handler == nil {
		return fmt.Errorf("no handler registered for task type: %s", t.TaskType)
	}

	// Log parameter info for debugging
	if t.Parameters == "" {
		fmt.Printf("Warning: Empty parameters for task %s\n", t.TaskID)
	} else if !strings.HasPrefix(t.Parameters, "{") {
		fmt.Printf("Warning: Non-JSON parameters for task %s: %s\n", t.TaskID, t.Parameters)
	}

	// Execute the task handler with the parameters
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

	// Update LastJobError with the new error information
	if errorObj != nil {
		t.LastJobError = &JobErrorReturn{
			ErrorObj:    errorObj,
			ErrorString: errorObj.Error(),
			RetryWorthy: true, // Assuming we want to retry if we're calling UpdateRetryConfig
		}
	} else {
		t.LastJobError = nil
	}

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

// CreateTask is a convenience function that creates a new task with the given parameters.
// This is the simplified client API function for creating parameter-based tasks
func CreateTask(taskID string, taskType string, parameters interface{}, maxRetries int, retryAfterHours float64) (*SnerdTask, error) {
	return NewSnerdTask(taskID, taskType, parameters, maxRetries, retryAfterHours)
}

// FromRetryableTask creates a SnerdTask from a RetryableTask
// This is used when loading tasks from the file store
func FromRetryableTask(rt *RetryableTask) *SnerdTask {
	// Create a new SnerdTask with the same core fields
	task := &SnerdTask{
		TaskID:          rt.TaskID,
		RetryCount:      rt.RetryCount,
		MaxRetries:      rt.MaxRetries,
		RetryAfterHours: rt.RetryAfterHours,
		RetryAfterTime:  rt.RetryAfterTime,
		TaskType:        rt.TaskType,
		LastErrorObj:    rt.LastErrorObj,
		LastJobError:    rt.LastJobError,
		CreatedAt:       rt.CreatedAt,
		UpdatedAt:       rt.UpdatedAt,
		DeletedAt:       rt.DeletedAt,
	}

	// Improved parameter extraction with better handling of nested JSON structures
	// Direct case: TaskData is already in the format we need
	if rt.TaskType != "" && strings.HasPrefix(rt.TaskData, "{") && !strings.Contains(rt.TaskData, "parameters") {
		// This is likely direct parameter data
		task.Parameters = rt.TaskData
		return task
	}

	// Complex case: need to extract parameters from nested structure
	var taskData map[string]interface{}
	if err := json.Unmarshal([]byte(rt.TaskData), &taskData); err == nil {
		// Look for the parameters field in the task data
		if params, ok := taskData["parameters"]; ok {
			// Convert parameters to usable format
			switch p := params.(type) {
			case string:
				// Already a string, use directly
				task.Parameters = p

			case map[string]interface{}:
				// Parameters are a nested map, marshal to JSON
				if paramsJSON, err := json.Marshal(p); err == nil {
					task.Parameters = string(paramsJSON)
				}

			default:
				// Try to marshal unknown type to JSON
				if paramsJSON, err := json.Marshal(params); err == nil {
					task.Parameters = string(paramsJSON)
				}
			}
		} else {
			// Special case: if no parameters field but we have URL or FilePath, create simple parameters
			if url, ok := taskData["url"].(string); ok && rt.TaskType == "http-fetch" {
				// Reconstruct http-fetch parameters
				params := map[string]string{"url": url}
				if paramsJSON, err := json.Marshal(params); err == nil {
					task.Parameters = string(paramsJSON)
					return task
				}
			} else if filePath, ok := taskData["filePath"].(string); ok && rt.TaskType == "file-read" {
				// Reconstruct file-read parameters
				params := map[string]string{"filePath": filePath}
				if paramsJSON, err := json.Marshal(params); err == nil {
					task.Parameters = string(paramsJSON)
					return task
				}
			}

			// As a last resort, use the entire TaskData as parameters
			task.Parameters = rt.TaskData
		}
	} else {
		// If we can't parse the TaskData as JSON, use it directly as parameters
		task.Parameters = rt.TaskData
	}

	return task
}

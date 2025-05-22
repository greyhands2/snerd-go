package queue

// Task represents a unit of work that can be processed by the queue system.
// Implement this interface to define your own task types.
type Task interface {
	// GetTaskID returns the unique identifier for the task.
	GetTaskID() string
	// GetRetryCount returns the number of times this task has been retried.
	GetRetryCount() int
	// Execute runs the task's logic. Return an error if the task fails and should be retried.
	Execute() error
}

// TaskWithMaxRetryCallback allows a task to handle the case where it has reached its maximum number of retries.
// Implement this interface if you want to perform custom logic (e.g., alerting, cleanup) when retries are exhausted.
type TaskWithMaxRetryCallback interface {
	// OnMaxRetryReached is called when the task reaches its maximum retry count.
	// The contextProvider function can be used to access additional context or task data.
	OnMaxRetryReached(contextProvider func() interface{}) error
}

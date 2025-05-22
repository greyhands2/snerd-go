package queue

type Task interface {
	GetTaskID() string
	GetRetryCount() int
	Execute() error // Clients implement this method
}

// TaskWithMaxRetryCallback is an interface for tasks that need to execute a special action when max retries is reached
type TaskWithMaxRetryCallback interface {
	// OnMaxRetryReached receives a function that provides context data
	OnMaxRetryReached(contextProvider func() interface{}) error
}

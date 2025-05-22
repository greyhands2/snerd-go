# snerd-go

[![Go Reference](https://pkg.go.dev/badge/github.com/greyhands2/snerd-go.svg)](https://pkg.go.dev/github.com/greyhands2/snerd-go)

A Go library for in-memory and persistent retryable task queues.

## Features
- In-memory and persistent (file-backed) queues
- Retryable tasks with configurable retry logic
- Task factories and extensibility for custom task types
- Periodic processing of retryable tasks using context and ticker

---

## Full Example: In-Memory Tasks

> **Note:** All examples use the import alias `snerd` for clarity.

```go
package main

import (
    "fmt"
    snerd "github.com/greyhands2/snerd-go"
)

type PrintTask struct {
    ID    string
    Count int
}

func (t *PrintTask) GetTaskID() string  { return t.ID }
func (t *PrintTask) GetRetryCount() int { return 0 }
func (t *PrintTask) Execute() error {
    fmt.Printf("[PrintTask] Executing task %s, count=%d\n", t.ID, t.Count)
    return nil
}

func main() {
    // Create an in-memory queue
    q := snerd.NewAnyQueue("example-queue", 10)

    // Enqueue tasks
    q.Enqueue(&PrintTask{ID: "task1", Count: 1})
    q.Enqueue(&PrintTask{ID: "task2", Count: 2})
    q.Enqueue(&PrintTask{ID: "task3", Count: 3})
    // Tasks are processed automatically in the background
}
```

---

## Full Example: Retryable Tasks (Sending Email)

```go
package main

import (
    "context"
    "fmt"
    "sync"
    "time"
    snerd "github.com/greyhands2/snerd-go"
)

type SendEmailTask struct {
    ID         string
    RetryCount int
    To         string
    Subject    string
    Body       string
}

func (t *SendEmailTask) GetTaskID() string  { return t.ID }
func (t *SendEmailTask) GetRetryCount() int { return t.RetryCount }
func (t *SendEmailTask) Execute() error {
    fmt.Printf("[SendEmailTask] Sending email to %s: %s\n%s\n", t.To, t.Subject, t.Body)
    // Simulate sending failure to trigger retry
    return fmt.Errorf("failed to send email (simulated)")
}

// Implement TaskWithMaxRetryCallback to handle max retries
func (t *SendEmailTask) OnMaxRetryReached(contextProvider func() interface{}) error {
    // The contextProvider can return the task itself or additional info
    failedEmail, ok := contextProvider().(*SendEmailTask)
    if !ok {
        fmt.Printf("[SendEmailTask] Max retries reached for task %s, but context is missing or wrong type.\n", t.ID)
        return nil
    }
    fmt.Printf("[SendEmailTask] Max retries reached for email to %s. Subject: '%s'. Body: '%s'\n", failedEmail.To, failedEmail.Subject, failedEmail.Body)
    // Here you could log to a file, alert an admin, etc.
    return nil
}

func main() {
    // Register your retryable task type if needed (see your library's registration system)
    // snerd.RegisterTaskType(...)

    // Create a queue for retryable tasks
    q := snerd.NewAnyQueue("retry-queue", 10)

    // Enqueue a retryable email task
    q.Enqueue(&SendEmailTask{
        ID:         "email1",
        RetryCount: 0,
        To:         "user@example.com",
        Subject:    "Welcome!",
        Body:       "Thanks for signing up.",
    })

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    var wg sync.WaitGroup

    // Start processing retryable tasks every 30 seconds
    snerd.ProcessRetryQueue(ctx, q, &wg, 30*time.Second)

    // Let it run for a while (for demo purposes)
    time.Sleep(2 * time.Minute)
    cancel()
    wg.Wait()
}
```

**Note:** The `OnMaxRetryReached` method will be called by the queue system when the maximum number of retries is reached for a task implementing `TaskWithMaxRetryCallback`. Here, we access the failed email's details from the context and log them, but you could also notify an admin or take other action.
---

## Example Usage

### 1. Import the library
```go
import (
    "context"
    "fmt"
    "sync"
    "time"
    snerd "github.com/greyhands2/snerd-go"
)
```

### 2. Define a Normal (In-Memory) Task
```go
// Implement the Task interface
 type PrintTask struct {
     ID    string
     Count int
 }

 func (t *PrintTask) GetTaskID() string    { return t.ID }
 func (t *PrintTask) GetRetryCount() int   { return 0 }
 func (t *PrintTask) Execute() error {
     fmt.Printf("[PrintTask] Executing task %s, count=%d\n", t.ID, t.Count)
     return nil
 }
```

### 3. Define a Retryable Task
```go
// Implement the Task interface and optionally TaskWithMaxRetryCallback
 type MyRetryableTask struct {
     ID         string
     RetryCount int
 }

 func (t *MyRetryableTask) GetTaskID() string  { return t.ID }
 func (t *MyRetryableTask) GetRetryCount() int { return t.RetryCount }
 func (t *MyRetryableTask) Execute() error {
     fmt.Printf("[MyRetryableTask] Executing retryable task %s, retry=%d\n", t.ID, t.RetryCount)
     // Return an error to trigger retry logic
     return fmt.Errorf("simulate failure")
 }
```

### 4. Create and Use a Queue
```go
// Create an in-memory queue
q := snerd.NewAnyQueue("example-queue", 10)

// Enqueue a normal task
q.Enqueue(&PrintTask{ID: "task1", Count: 1})

// Enqueue a retryable task (using your factory/registration system)
// See your codebase for registering custom retryable task types
```

### 5. Process In-Memory Tasks
For in-memory tasks, simply calling `Enqueue()` is sufficient—the queue will process the task immediately in the background. No background processor or polling is needed!

```go
q.Enqueue(&PrintTask{ID: "task1", Count: 1})
q.Enqueue(&PrintTask{ID: "task2", Count: 2})
// ...
```

If you want to monitor or synchronize task completion, you can use your own `sync.WaitGroup` or other mechanism.

### 6. Process Retryable Tasks Periodically

You can process retryable tasks continuously by running a background goroutine (as above), or you can use a cron job or external scheduler to trigger processing at fixed intervals.

#### Example: Using a Go Cron Library
```go
import "github.com/robfig/cron/v3"

c := cron.New()
c.AddFunc("@every 1m", func() {
    // This will run every minute
    snerd.ProcessDueTasks() // or your own wrapper for processing retryable tasks
})
c.Start()
// ...
// To stop cron:
c.Stop()
```

#### Example: Using a Shell Cron Job
You can also run a Go program or script that processes retryable tasks from your system's cron:

```
* * * * * /usr/local/bin/my-retryable-task-processor
```

Where `my-retryable-task-processor` is a Go program that calls `snerd.ProcessDueTasks()` or similar logic once per invocation.

#### Example: Continuous Background Processing (as before)
```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
var wg sync.WaitGroup

// Start processing retryable tasks every 30 seconds
snerd.ProcessRetryQueue(ctx, q, &wg, 30*time.Second)

// ...
// To stop processing:
cancel()
wg.Wait()
```

---

## Notes
- **For in-memory tasks:** Just call `Enqueue()`—no periodic processor is needed, as tasks are run immediately in the background.
- **For persistent retryable tasks:** Implement and register your custom task types using the factory/registry pattern provided in this library, and use a periodic processor (e.g., `ProcessRetryQueue`).
- For continuous background processing of retryable tasks, you can use a Go cron library (like `robfig/cron`) or a system cron job to periodically trigger processing.
- See the code for `FetchDueTasks`, `ProcessDueTasks`, and task registration for more advanced usage.
- You can customize the polling interval for retryable task processing.

---

## License
MIT

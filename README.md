# snerd-go

[![Go Reference](https://pkg.go.dev/badge/github.com/greyhands2/snerd-go.svg)](https://pkg.go.dev/github.com/greyhands2/snerd-go)

A Go library for parameter-based task execution with persistent storage and automatic retries.

## Features
- Parameter-based task execution system (no need to implement interfaces)
- Persistent task storage in a hidden `.snerdata` folder
- Automatic task retries with configurable retry logic
- Centralized task handler registration
- Error tracking with detailed failure information
- Background processing of tasks
- Proper task deletion after max retries

---

## Quick Start Example

Here's a simple example of how to use snerd-go for processing tasks in the background with automatic retry:

```go
package main

import (
	"encoding/json"
	"fmt"
	"time"

	snerd "github.com/greyhands2/snerd-go"
)

func main() {
	// 1. Register task handlers
	snerd.RegisterTaskHandler("http-fetch", func(parameters string) error {
		var params struct {
			URL string `json:"url"`
		}
		json.Unmarshal([]byte(parameters), &params)
		
		fmt.Printf("Fetching URL: %s\n", params.URL)
		// ... actual HTTP logic here ...
		return nil
	})
	
	// 2. Register max retry handler (optional)
	snerd.RegisterMaxRetryHandler("http-fetch", func(parameters string) error {
		var params struct {
			URL string `json:"url"`
		}
		json.Unmarshal([]byte(parameters), &params)
		
		fmt.Printf("Max retries reached for URL: %s\n", params.URL)
		return nil
	})
	
	// 3. Create a queue
	queue := snerd.NewAnyQueue("my-queue", 10)
	
	// 4. Create and enqueue a task
	task, _ := snerd.CreateTask(
		"task-123",         // Unique task ID
		"http-fetch",       // Task type
		map[string]string{
			"url": "https://example.com",
		},
		5,                  // Max retries
		0.25,               // Retry after hours (15 mins)
	)
	
	queue.Enqueue(task)
	
	// Tasks will be processed in the background
	time.Sleep(time.Minute * 5)
}
```

## Example: Email Sending Task

Email sending is a perfect use case for the task system as it can be retried if the mail server is temporarily unavailable.

### 1. Register the Email Task Handler

```go
package main

import (
    "encoding/json"
    "fmt"
    "net/smtp"
    "time"
    snerd "github.com/greyhands2/snerd-go"
)

// Register the task handler for sending emails
func registerEmailHandler() {
    snerd.RegisterTaskHandler("email-send", func(parameters string) error {
        // Parse parameters
        var params struct {
            To      string `json:"to"`
            From    string `json:"from"`
            Subject string `json:"subject"`
            Body    string `json:"body"`
            SMTP    struct {
                Server   string `json:"server"`
                Port     int    `json:"port"`
                Username string `json:"username"`
                Password string `json:"password"`
            } `json:"smtp"`
        }
        if err := json.Unmarshal([]byte(parameters), &params); err != nil {
            return fmt.Errorf("failed to parse email parameters: %w", err)
        }
        
        // Create email message
        message := fmt.Sprintf("From: %s\nTo: %s\nSubject: %s\n\n%s",
            params.From, params.To, params.Subject, params.Body)
            
        // Set up SMTP authentication
        auth := smtp.PlainAuth("", params.SMTP.Username, params.SMTP.Password, params.SMTP.Server)
        
        // Connect to the server and send the email
        addr := fmt.Sprintf("%s:%d", params.SMTP.Server, params.SMTP.Port)
        err := smtp.SendMail(addr, auth, params.From, []string{params.To}, []byte(message))
        if err != nil {
            return fmt.Errorf("failed to send email: %w", err)
        }
        
        fmt.Printf("Email sent successfully to %s\n", params.To)
        return nil
    })
    
    // Register max retry handler
    snerd.RegisterMaxRetryHandler("email-send", func(parameters string) error {
        var params struct {
            To      string `json:"to"`
            Subject string `json:"subject"`
        }
        if err := json.Unmarshal([]byte(parameters), &params); err != nil {
            return err
        }
        
        fmt.Printf("Failed to send email to %s with subject '%s' after multiple attempts\n", 
            params.To, params.Subject)
        // Here you could log to a database or notify an admin
        return nil
    })
}
```

### 2. Creating and Enqueuing an Email Task

```go
// Create and enqueue an email task
func sendWelcomeEmail(userEmail, userName string) error {
    // Create the email task
    emailTask, err := snerd.CreateTask(
        "welcome-"+userEmail,  // Unique task ID
        "email-send",         // Task type
        map[string]interface{}{
            "to":      userEmail,
            "from":    "welcome@example.com",
            "subject": "Welcome to Our Service",
            "body":    fmt.Sprintf("Hello %s,\n\nWelcome to our service! We're excited to have you on board.\n\nRegards,\nThe Team", userName),
            "smtp": map[string]interface{}{
                "server":   "smtp.example.com",
                "port":     587,
                "username": "smtp-user",
                "password": "smtp-password", // In production, use a secure way to handle credentials
            },
        },
        5,     // Max retries
        0.25,  // Retry interval in hours (15 minutes)
    )
    if err != nil {
        return fmt.Errorf("failed to create email task: %w", err)
    }
    
    // Get the queue and enqueue the task
    queue := snerd.NewAnyQueue("email-queue", 100)
    return queue.EnqueueSnerdTask(emailTask)
}
```

## Example: Task Retry and Deletion Behavior

The snerd-go library handles automatic retries for failed tasks and proper task deletion once the maximum retry count is reached.

### Task Lifecycle

1. When a task fails, it is scheduled for retry based on the configured retry interval
2. The retry count is incremented with each attempt
3. Once the retry count reaches the maximum, the task is deleted and the max retry handler is called
4. Deleted tasks are filtered out and will never be retried again

### Task Deletion Example

Here's an example of how snerd-go handles task retry and deletion, using an HTTP fetch task that will fail:

```go
package main

import (
	"encoding/json"
	"fmt"
	snerd "github.com/greyhands2/snerd-go"
	"time"
)

func main() {
	// Register HTTP task handler
	snerd.RegisterTaskHandler("http-fetch", func(parameters string) error {
		var params struct {
			URL string `json:"url"`
		}
		json.Unmarshal([]byte(parameters), &params)
		
		// This will fail because the domain doesn't exist
		fmt.Printf("[http-fetch] Fetching URL: %s\n", params.URL)
		return fmt.Errorf("error making HTTP request: domain not found")
	})
	
	// Register handler for when max retries is reached
	snerd.RegisterMaxRetryHandler("http-fetch", func(parameters string) error {
		var params struct {
			URL string `json:"url"`
		}
		json.Unmarshal([]byte(parameters), &params)
		
		fmt.Printf("[http-fetch] Max retries reached for URL: %s\n", params.URL)
		return nil
	})
	
	// Create and start a queue
	queue := snerd.NewAnyQueue("my-queue", 10)
	
	// Create a task that will fail
	task, _ := snerd.CreateTask(
		"http1",
		"http-fetch",
		map[string]string{
			"url": "https://non-existent-domain-123456.com/get",
		},
		5,      // Max retries
		0.0083, // Retry after ~30 seconds
	)
	
	// Enqueue the task
	queue.Enqueue(task)
	
	// Wait for the task to be processed and reach max retries
	time.Sleep(time.Second * 180)
}
```

With this setup:

1. The task will be retried 5 times (the maximum retry count)
2. Each retry will occur approximately 30 seconds after the previous attempt
3. After the 5th retry, the task will be marked as deleted and never processed again
4. The max retry handler will be called to perform any cleanup or notification

### Task Storage

All tasks are stored in a log file under the `.snerdata/tasks/` directory. The file uses an append-only log format where each task operation (creation, update, deletion) is written as a new JSON line. The file is compacted periodically to remove old entries for deleted tasks.

Example task log entry for a task that has reached max retries and been deleted:

```json
{"taskId":"http1","retryCount":5,"maxRetries":5,"retryAfterHours":0.0083,"retryAfterTime":"2025-07-02T23:07:41.901013+01:00","taskData":"{\"deletedAt\":\"2025-07-02T23:07:51+01:00\",\"lastError\":\"error making HTTP request: Get \\\"https://non-existent-domain-123456.com/get\\\": dial tcp: lookup non-existent-domain-123456.com: no such host\",\"lastErrorObj\":null,\"lastErrorTime\":\"2025-07-02T23:07:41+01:00\",\"lastJobError\":null,\"maxRetries\":5,\"parameters\":\"{\\\"url\\\":\\\"https://non-existent-domain-123456.com/get\\\"}\",\"retryAfterHours\":0.0083,\"retryAfterTime\":\"2025-07-02T23:07:41+01:00\",\"retryCount\":5,\"taskId\":\"http1\",\"taskType\":\"http-fetch\"}","taskType":"http-fetch","LastErrorObj":null,"LastJobError":{"error":"error making HTTP request: Get \"https://non-existent-domain-123456.com/get\": dial tcp: lookup non-existent-domain-123456.com: no such host","retry_worthy":true}}
```

## Example: Image Processing Task

Image processing is a resource-intensive operation that's well-suited for background processing.

### 1. Register the Image Processing Handler

```go
package main

import (
    "encoding/json"
    "fmt"
    "image"
    "image/jpeg"
    "image/png"
    "os"
    "path/filepath"
    "strings"
    
    "github.com/nfnt/resize"
    snerd "github.com/greyhands2/snerd-go"
)

// Register handlers for image processing
func registerImageHandlers() {
    // Handler for resizing images
    snerd.RegisterTaskHandler("image-resize", func(parameters string) error {
        // Parse parameters
        var params struct {
            SourcePath string `json:"sourcePath"`
            TargetPath string `json:"targetPath"`
            Width      uint   `json:"width"`
            Height     uint   `json:"height"`
            Quality    int    `json:"quality"`
        }
        if err := json.Unmarshal([]byte(parameters), &params); err != nil {
            return fmt.Errorf("failed to parse image parameters: %w", err)
        }
        
        // Open the source image
        file, err := os.Open(params.SourcePath)
        if err != nil {
            return fmt.Errorf("failed to open source image: %w", err)
        }
        defer file.Close()
        
        // Decode the image
        img, format, err := image.Decode(file)
        if err != nil {
            return fmt.Errorf("failed to decode image: %w", err)
        }
        
        // Resize the image
        resized := resize.Resize(params.Width, params.Height, img, resize.Lanczos3)
        
        // Ensure target directory exists
        targetDir := filepath.Dir(params.TargetPath)
        if err := os.MkdirAll(targetDir, 0755); err != nil {
            return fmt.Errorf("failed to create target directory: %w", err)
        }
        
        // Create the output file
        out, err := os.Create(params.TargetPath)
        if err != nil {
            return fmt.Errorf("failed to create output file: %w", err)
        }
        defer out.Close()
        
        // Save the resized image
        switch strings.ToLower(format) {
        case "jpeg", "jpg":
            err = jpeg.Encode(out, resized, &jpeg.Options{Quality: params.Quality})
        case "png":
            err = png.Encode(out, resized)
        default:
            return fmt.Errorf("unsupported image format: %s", format)
        }
        
        if err != nil {
            return fmt.Errorf("failed to encode output image: %w", err)
        }
        
        fmt.Printf("Successfully resized image from %s to %s\n", 
            params.SourcePath, params.TargetPath)
        return nil
    })
}
```

### 2. Creating and Enqueuing an Image Processing Task

```go
// Function to create and enqueue an image processing task
func resizeUserProfileImage(originalPath, userId string) error {
    // Prepare the target paths for different sizes
    basePath := filepath.Dir(originalPath)
    fileName := filepath.Base(originalPath)
    extension := filepath.Ext(fileName)
    nameWithoutExt := strings.TrimSuffix(fileName, extension)
    
    // Create paths for thumbnail and medium-sized images
    thumbnailPath := filepath.Join(basePath, nameWithoutExt + "-thumb" + extension)
    mediumPath := filepath.Join(basePath, nameWithoutExt + "-medium" + extension)
    
    // Create thumbnail task
    thumbTask, err := snerd.CreateTask(
        "thumb-" + userId,
        "image-resize",
        map[string]interface{}{
            "sourcePath": originalPath,
            "targetPath": thumbnailPath,
            "width":      100,
            "height":     100,
            "quality":    85,
        },
        3,      // Max retries
        0.0083, // Retry interval (30 seconds)
    )
    if err != nil {
        return fmt.Errorf("failed to create thumbnail task: %w", err)
    }
    
    // Create medium-size task
    mediumTask, err := snerd.CreateTask(
        "medium-" + userId,
        "image-resize",
        map[string]interface{}{
            "sourcePath": originalPath,
            "targetPath": mediumPath,
            "width":      400,
            "height":     0,  // 0 means maintain aspect ratio
            "quality":    90,
        },
        3,      // Max retries
        0.0083, // Retry interval (30 seconds)
    )
    if err != nil {
        return fmt.Errorf("failed to create medium-size task: %w", err)
    }
    
    // Get queue and enqueue both tasks
    queue := snerd.NewAnyQueue("image-queue", 100)
    if err := queue.EnqueueSnerdTask(thumbTask); err != nil {
        return fmt.Errorf("failed to enqueue thumbnail task: %w", err)
    }
    if err := queue.EnqueueSnerdTask(mediumTask); err != nil {
        return fmt.Errorf("failed to enqueue medium-size task: %w", err)
    }
    
    fmt.Printf("Enqueued image processing tasks for user %s\n", userId)
    return nil
}
```

## Example: Using Both Task Types Together

Here's how you might use both email and image processing tasks in a user registration flow:

```go
func main() {
    // Register all task handlers
    registerEmailHandler()
    registerImageHandlers()
    
    // Start the application
    fmt.Println("Starting application with background task processing...")
    
    // Simulate user registration with profile image upload
    userId := "user123"
    userEmail := "new.user@example.com"
    userName := "John Doe"
    profileImagePath := "/uploads/original/profile123.jpg"
    
    // Process the user's profile image
    if err := resizeUserProfileImage(profileImagePath, userId); err != nil {
        fmt.Printf("Error processing profile image: %v\n", err)
    }
    
    // Send welcome email
    if err := sendWelcomeEmail(userEmail, userName); err != nil {
        fmt.Printf("Error sending welcome email: %v\n", err)
    }
    
    fmt.Println("User registration complete. Background tasks will process automatically.")
    
    // In a real application, you would keep the server running
    // Here we just wait a bit to let tasks process
    time.Sleep(time.Second * 30)
    fmt.Println("Application shutting down.")
}
```

---

## Creating a Non-Retryable Task

To create a task that executes exactly once and is not retried on failure, simply set `MaxRetries` to 0:

```go
// Create a non-retryable task
nonRetryableTask, err := snerd.CreateTask(
    "oneshot1",
    "send-notification",
    map[string]string{
        "message": "This is a one-time notification",
        "channel": "alerts",
    },
    0,      // MaxRetries = 0 means no retries
    0,      // RetryInterval doesn't matter for non-retryable tasks
)
if err != nil {
    panic(err)
}
queue.EnqueueSnerdTask(nonRetryableTask)
```

**Note:** Even non-retryable tasks are stored in the file store for audit and recovery purposes. They'll be deleted automatically after execution.
---

## API Reference

### Registering Task Handlers

Register handlers for your task types at application startup:

```go
// Register a task handler for "email-send" tasks
snerd.RegisterTaskHandler("email-send", func(parameters string) error {
    // Parse parameters from JSON string
    var params struct {
        To      string `json:"to"`
        Subject string `json:"subject"`
        Body    string `json:"body"`
    }
    if err := json.Unmarshal([]byte(parameters), &params); err != nil {
        return err
    }
    
    // Task implementation logic
    // ...
    return nil
})
```

### Registering Max Retry Handlers (Optional)

Optionally register handlers for when tasks reach their maximum retry limit:

```go
// Register a max retry handler for "email-send" tasks
snerd.RegisterMaxRetryHandler("email-send", func(parameters string) error {
    var params struct {
        To      string `json:"to"`
        Subject string `json:"subject"`
    }
    if err := json.Unmarshal([]byte(parameters), &params); err != nil {
        return err
    }
    
    // Log or notify about the permanently failed task
    fmt.Printf("Failed to send email to %s after max retries\n", params.To)
    return nil
})
```

### Creating Tasks

Create tasks with parameters using the CreateTask helper function:

```go
// Create a task with parameters
task, err := snerd.CreateTask(
    "unique-task-id",     // Unique identifier for this task
    "task-type",          // Task type (must match a registered handler)
    map[string]string{    // Parameters for the task
        "key1": "value1",
        "key2": "value2",
    },
    5,                    // Maximum number of retries (0 for non-retryable)
    0.25,                 // Retry interval in hours (0.25 = 15 minutes)
)
```

### Enqueueing Tasks

Add tasks to the queue for execution:

```go
// Create a queue
queue := snerd.NewAnyQueue("my-queue", 10)

// Enqueue the task
queue.EnqueueSnerdTask(task)
```

### Error Handling

Error information is automatically tracked for failed tasks:

1. When a task fails, the error is captured and stored with the task
2. The retry count is incremented
3. The task is scheduled for retry after the specified interval
4. If max retries is reached, the max retry handler is called (if registered)

### Task Storage

All tasks are stored in a hidden `.snerdata` folder for persistence. This ensures:

1. Tasks survive application restarts
2. Failed tasks can be retried later
3. Task execution history is preserved

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

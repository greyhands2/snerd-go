<div align="center">
  <img src="./assets/Designer-9.png" height="120" alt="Snerd-Go Logo" />
  <h1>⚙️ snerd-go v1.0.2</h1>
  <p>A blazingly fast, brutally simple, zero-dependency embedded background job engine for Go.</p>

  [![Go Reference](https://pkg.go.dev/badge/github.com/greyhands2/snerd-go.svg)](https://pkg.go.dev/github.com/greyhands2/snerd-go)
</div>

If you are tired of wrestling with heavy, bloated background job frameworks like Redis, Postgres tables, or RabbitMQ just to send a few emails in the background... well, you are in the right place. 

`snerd-go` is an embedded, high-performance background task queue that lives entirely in a single, perfectly OS-locked, append-only `.log` file on your file system. It was designed to bring aggressive concurrency and a lightweight footprint to your Go microservices.

No databases. No external daemons. No nonsense.

---

## 🔥 v1.0.2 AI Features
* **Zero External Infrastructure**: You don't need a Redis cluster. Your tasks are persisted directly to `.snerdata/tasks/tasks.log` using standard filesystem I/O.
* **Bulletproof File Locks**: Safely scales across multiple processes! We utilize OS-level file-locking boundaries to guarantee that your tasks are never corrupted.
* **Smart API Rate-Limiting**: Natively tracks `rateLimitGroup` execution velocity to prevent 429 "Too Many Requests" API errors.
* **Payload-Hashing Deduplication**: Automatically computes cryptographic hashes to drop duplicate tasks instantly.
* **Dynamic Float Prioritization**: A native Binary Max-Heap bypasses standard FIFO rules for high urgency tasks.
* **Dead-Letter Queue (DLQ)**: Built-in `maxRetries` limits and hooks to elegantly catch and bury poison-pill tasks.

---

## 📦 Installation

```bash
go get github.com/greyhands2/snerd-go
```

---

## 🚀 Quickstart

It takes roughly 3 lines of code to spin up a queue and start firing background jobs. 

```go
package main

import (
	"encoding/json"
	"fmt"
	"time"

	snerd "github.com/greyhands2/snerd-go"
)

func main() {
	// 1. Create the Queue (automatically handles FileStore initialization)
	// You can pass a custom storage path or use the default ".snerdata"
	queue := snerd.NewAnyQueue("my-fast-queue", 10)

	// 2. Register your Task Handler (The closure that does the actual work)
	snerd.RegisterTaskHandler("generate_ai_image", func(parameters string) error {
		var data map[string]string
		json.Unmarshal([]byte(parameters), &data)
		fmt.Printf("Generating image with prompt: %s\n", data["prompt"])
		
		// do your heavy lifting here!
		// return fmt.Errorf("...") to trigger a retry!
		return nil
	})

	// 3. (Optional) Register a Dead-Letter Handler for when retries run out
	snerd.RegisterMaxRetryHandler("generate_ai_image", func(parameters string) error {
		fmt.Printf("Task permanently failed! Payload: %s\n", parameters)
		return nil
	})

	// 4. Boot the background processor polling loop
	queue.SetProcessorInterval(time.Second * 2)

	// 5. Enqueue a task!
	rateLimitGroup := "openai_api"
	maxPerMinute := 50
	autoDedupe := true
	urgencyScore := 0.95
	cronStr := "1h"
	webhookUrl := "https://api.example.com/webhook"

	task, _ := snerd.NewSnerdTaskAdvanced(
		"unique-task-id-123",  // Unique task ID
		"generate_ai_image",   // Task type (matches handler)
		map[string]string{"prompt": "A crab in space"}, // JSON Payload
		3,                     // Max retries
		1.0,                   // Delay in hours for retries
		&rateLimitGroup,       // Rate limit group
		&maxPerMinute,         // Max requests per minute
		&autoDedupe,           // Auto-dedupe
		&urgencyScore,         // Urgency score
		nil,                   // Execute at timestamp
		&cronStr,              // Cron: Runs every 1 hour!
		&webhookUrl,           // Webhook URL
	)

	queue.Enqueue(task)

	// Keep your app alive
	time.Sleep(time.Minute)
}
```

---

### ⚙️ Advanced Task Configuration (v1.0.2)
To power complex AI workflows, tasks can now be configured with advanced orchestration parameters via `NewSnerdTaskAdvanced`:

* **`autoDedupe` (`*bool`)**: If set to `true`, the daemon computes a cryptographic hash of the `taskType` and `parameters`. If an identical payload is currently sitting in the queue pending execution, this new task is silently dropped. Excellent for preventing duplicate generative AI requests from trigger-happy users!
* **`urgencyScore` (`*float64`)**: A value (e.g. `0.99`) used to bypass the standard FIFO queue. SnerdMQ uses a true Binary Max-Heap to continually float tasks with the highest urgency score to the very front of the execution line. Standard tasks default to `0.0`.
* **`rateLimitGroup` (`*string`)**: A custom string (e.g. `"openai_api"` or `"db_writes"`) that groups tasks together for backpressure control.
* **`maxPerMinute` (`*int`)**: Used in conjunction with `rateLimitGroup`. If the queue processes more tasks in this group than the allowed limit within a 60-second rolling window, further tasks in this group are temporarily paused. This natively prevents 429 "Too Many Requests" errors when bursting third-party APIs.
* **`executeAt` (`*time.Time` | `*string`)**: A timestamp of when the job should be executed in the future.
* **`cron` (`*string`)**: A cron expression (e.g. `"0 * * * *"`) for recurring jobs. Shorthands like `"2h"` or `"10m"` are also supported.
* **`webhookUrl` (`*string`)**: By providing a webhook URL, SnerdQueue will completely bypass your local Go handlers and dispatch the task payload via an HTTP POST request directly to the specified URL.

### 🌐 HTTP Webhooks (Serverless Execution)
You can configure a task to execute externally via an HTTP POST request. By setting a `webhookUrl`, the internal background processor will skip any registered handlers (`snerd.RegisterTaskHandler`) and directly invoke the HTTP endpoint.

If the HTTP endpoint returns a non-200 status code, it triggers a retry. If it permanently fails (reaches `maxRetries`), the Dead Letter Queue event is automatically fired via a final HTTP POST to the same `webhookUrl` but with the header `X-SnerdMQ-Event: MaxRetriesReached`.

### 🕒 Cron Jobs vs. Retryable Jobs
When using the new scheduling features, it is important to understand the difference between Cron and Retry behaviors:
> - **A Cron Job** is a *Repeatable Job* that executes again **only after a success**, on a fixed schedule.
> - **A Retryable Job** is a *Recovery Job* that executes again **only after a failure**, attempting to recover using the `retryAfterHours` backoff.
> - **Combined:** If a Cron Job fails, it temporarily uses `retryAfterHours` to retry until it recovers. Once it succeeds, it goes back to ticking on its standard cron schedule!

## 🧠 Architecture Details

`snerd-go` utilizes an **Append-Only Log Model** to achieve massive write speeds.
Instead of updating rows in a database, every time a task is enqueued, updated, or deleted, a brand new JSON line is instantly appended to the end of the log file.

When the Queue wakes up on its polling interval, it scans the log, maps out the absolute latest state of every task, and spawns parallel Goroutines for anything that is currently due (`retryAfterTime <= now`). 

If your file ever grows too large, `snerd-go` atomically clones, shrinks, and replaces the file in the background (Log Compaction) to keep disk space minimal.

---

## 🤝 License

MIT License. Do whatever you want with it, just don't let your tasks die unhandled.

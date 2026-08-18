package snerd

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// AnyQueue is a thread-safe queue that manages SnerdTask execution, retry logic, and statistics

// PriorityQueue implements heap.Interface and holds RetryableTasks
type PriorityQueue []*RetryableTask

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	scoreI := 0.0
	if pq[i].UrgencyScore != nil {
		scoreI = *pq[i].UrgencyScore
	}
	scoreJ := 0.0
	if pq[j].UrgencyScore != nil {
		scoreJ = *pq[j].UrgencyScore
	}
	// Max-Heap: greater score should be closer to root (Less returns true if i should pop BEFORE j)
	return scoreI > scoreJ
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*RetryableTask)
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

// AnyQueue is a thread-safe queue that manages SnerdTask execution, retry logic, and statistics
type AnyQueue struct {
	name            string
	maxSize         int
	mu              sync.Mutex
	totalEnqueued   int64
	totalDequeued   int64
	fileStore       *FileStore
	rateLimiter     *RateLimiter
	processorActive bool
	processorCtx    context.Context
	processorCancel context.CancelFunc
	activeHashes    map[string]bool
	hashMu          sync.Mutex
	executingTasks  map[string]bool
	execMu          sync.Mutex
	workerPool      chan struct{}
	progressSubs    []chan string
	progressMu      sync.Mutex
}

// TaskFactory creates a Task from its stored data.
// The factory function is responsible for reconstructing a Task instance, including unmarshaling any stored data.
type TaskFactory func(id string, data string) (Task, error)

// NewAnyQueue creates a new queue with the given parameters
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

	// Initialize the file store
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

	initialHashes := make(map[string]bool)
	if fileStore != nil {
		if tasks, rErr := fileStore.ReadTasks(); rErr == nil {
			for _, task := range tasks {
				if (task.DeletedAt == nil || task.DeletedAt.IsZero()) && task.PayloadHash != nil {
					initialHashes[*task.PayloadHash] = true
				}
			}
		}
	}

	// Create the queue with the specified parameters
	q := &AnyQueue{
		name:            name,
		maxSize:         maxSize,
		processorActive: false,
		executingTasks:  make(map[string]bool),
		activeHashes:    initialHashes,
		fileStore:       fileStore,
		rateLimiter:     NewRateLimiter(filepath.Dir(taskStorePath)),
		workerPool:      make(chan struct{}, 100),
		progressSubs:    make([]chan string, 0),
	}

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

// Enqueue adds a task to the queue
func (q *AnyQueue) Enqueue(task Task) error {
	// Handle SnerdTask directly
	if snerdTask, ok := task.(*SnerdTask); ok {
		return q.EnqueueSnerdTask(snerdTask)
	}

	// For any non-SnerdTask, return an error as we no longer support legacy task types
	return fmt.Errorf("legacy task types are no longer supported; use SnerdTask instead")
}

// EnqueueSnerdTask adds a parameter-based SnerdTask to the queue for execution
// This is the preferred method for adding new tasks as it uses the parameter-based
// approach that doesn't require client-side task registration
func (q *AnyQueue) EnqueueSnerdTask(task *SnerdTask) error {
	if task.PayloadHash != nil {
		q.hashMu.Lock()
		if q.activeHashes[*task.PayloadHash] {
			q.hashMu.Unlock()
			fmt.Printf("Duplicate task detected (hash: %s), silently dropping.\n", *task.PayloadHash)
			return nil
		}
		q.activeHashes[*task.PayloadHash] = true
		q.hashMu.Unlock()
	}

	// Convert the SnerdTask to a RetryableTask for storage
	zeroTime := time.Time{}
	task.DeletedAt = &zeroTime
	retryableTask := task.ToRetryableTask()

	// Store the task in the file store
	if q.fileStore != nil {
		if err := q.fileStore.CreateTask(retryableTask); err != nil {
			return fmt.Errorf("failed to store task: %w", err)
		}
	}

	// Update queue stats
	atomic.AddInt64(&q.totalEnqueued, 1)

	// Try to acquire a worker slot
	select {
	case q.workerPool <- struct{}{}:
		// Process immediately if the task is due
		now := time.Now().UTC()
		if (task.RetryAfterTime.Before(now) || task.RetryAfterTime.Equal(now)) && (task.ExecuteAt.Before(now) || task.ExecuteAt.Equal(now)) {
			q.execMu.Lock()
			if q.executingTasks[task.GetTaskID()] {
				q.execMu.Unlock()
				<-q.workerPool
				return nil
			}
			q.executingTasks[task.GetTaskID()] = true
			q.execMu.Unlock()

			go func() {
				defer func() {
					q.execMu.Lock()
					delete(q.executingTasks, task.GetTaskID())
					q.execMu.Unlock()
				}()
				// Create context
				var ctx context.Context
				var cancel context.CancelFunc
				if task.MaxExecutionSeconds != nil {
					ctx, cancel = context.WithTimeout(context.Background(), time.Duration(*task.MaxExecutionSeconds)*time.Second)
				} else {
					ctx, cancel = context.WithCancel(context.Background())
				}
				defer cancel()

				// Execute the task
				if err := task.Execute(ctx); err != nil {
					// Update retry configuration if we haven't exceeded max retries
					fmt.Println("THERE WAS AN ERROR", task.RetryCount, task.MaxRetries)
					if task.RetryCount < task.MaxRetries {
						// Let the FileStore update the retry config and calculate the next retry time
						// The UpdateTaskRetryConfig method will handle:
						// 1. Incrementing retry count
						// 2. Calculating proper next retry time
						// 3. Storing the error information
						if q.fileStore != nil {
							if updateErr := q.fileStore.UpdateTaskRetryConfig(task.GetTaskID(), err); updateErr != nil {
								fmt.Printf("Error updating task retry config: %v\n", updateErr)
							} else {
								// Calculate and display when the next retry will happen
								task.RetryCount++ // Local update for logging only
								retryTime := time.Now().Add(time.Duration(task.RetryAfterHours * float64(time.Hour)))
								fmt.Printf("Task %s failed with error: %v\n", task.GetTaskID(), err)
								fmt.Printf("Scheduled for retry %d/%d at %s\n",
									task.RetryCount, task.MaxRetries, retryTime.Format(time.RFC3339))
							}
						} else {
							// No filestore available, log an error
							fmt.Printf("Warning: Cannot update task %s - no file store available\n", task.GetTaskID())
						}
					} else {
						// Task reached max retries
						if callbackErr := task.OnMaxRetryReached(ctx, nil); callbackErr != nil {
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
						rescheduled := false
						if task.CronExpr != nil && *task.CronExpr != "" {
							parser := cronParser()
							if sched, err := parser.Parse(*task.CronExpr); err == nil {
								task.ExecuteAt = sched.Next(time.Now().UTC())
								task.RetryCount = 0
								task.LastErrorObj = nil
								task.LastJobError = nil
								if saveErr := q.fileStore.CreateTask(task.ToRetryableTask()); saveErr != nil {
									fmt.Printf("Error rescheduling cron task: %v\n", saveErr)
								} else {
									rescheduled = true
								}
							}
						}

						if !rescheduled {
							if deleteErr := q.fileStore.DeleteTask(task.GetTaskID()); deleteErr != nil {
								fmt.Printf("Error deleting task: %v\n", deleteErr)
							} else {
								if task.PayloadHash != nil {
									q.hashMu.Lock()
									delete(q.activeHashes, *task.PayloadHash)
									q.hashMu.Unlock()
								}
							}
						}
					}
					atomic.AddInt64(&q.totalDequeued, 1)
				}
			}()
		} else {
			<-q.workerPool
		}
	default:
		// Pool is saturated, let processDueTasks handle it when a slot frees up
	}

	return nil
}

func (q *AnyQueue) processTask(task Task) {
	// Create context (default empty context since this is an internal bypass)
	ctx := context.Background()

	// Just execute the task directly
	if err := task.Execute(ctx); err != nil {
		fmt.Printf("error executing task: %v\n", err)
	}
}

// ProcessDueTasks processes all tasks that are due for execution (retry time has passed)..
func (q *AnyQueue) ProcessDueTasks() {
	// Step 1: Load all tasks from the FileStore
	if q.fileStore == nil {
		fmt.Println("No file store available for processing tasks")
		return
	}

	fmt.Println("Processing due tasks...")
	tasks, err := q.fileStore.ReadDueTasks()
	if err != nil {
		fmt.Printf("Error reading tasks: %v\n", err)
		return
	}

	// No need to filter here, ReadDueTasks already returns only due tasks
	if len(tasks) == 0 {
		fmt.Println("No due tasks found")
		return
	}

	fmt.Printf("Found %d due tasks\n", len(tasks))

	// Step 2: Construct PriorityQueue (Max-Heap)
	pq := make(PriorityQueue, len(tasks))
	for i, t := range tasks {
		pq[i] = t
	}
	heap.Init(&pq)

	available := cap(q.workerPool) - len(q.workerPool)
	if available <= 0 {
		return
	}

	// Step 3: Process due tasks in priority order up to available limit
	for i := 0; i < available && pq.Len() > 0; i++ {
		t := heap.Pop(&pq).(*RetryableTask)

		// Wait for a worker slot
		q.workerPool <- struct{}{}

		// Convert RetryableTask to SnerdTask for execution
		snerdTask := FromRetryableTask(t)

		// Skip tasks with missing type or parameters
		if snerdTask.TaskType == "" {
			fmt.Printf("Skipping task %s: missing task type\n", snerdTask.GetTaskID())
			<-q.workerPool
			continue
		}

		// Log task execution for debugging
		fmt.Printf("Executing task %s (type=%s)\n", snerdTask.GetTaskID(), snerdTask.TaskType)

		// Get the task handler from the registry
		handlersMutex.RLock()
		handler, exists := taskHandlers[snerdTask.TaskType]
		handlersMutex.RUnlock()

		if !exists || handler == nil {
			fmt.Printf("No handler registered for task type: %s\n", snerdTask.TaskType)
			continue
		}

		// Execute the handler with the task parameters
		fmt.Printf("Task parameters: %s\n", snerdTask.Parameters)

		// Check Rate Limits before executing
		if snerdTask.RateLimitGroup != nil && snerdTask.MaxPerMinute != nil {
			if !q.rateLimiter.CheckLimit(*snerdTask.RateLimitGroup, *snerdTask.MaxPerMinute) {
				snerdTask.RetryAfterTime = time.Now().Add(60 * time.Second)
				if q.fileStore != nil {
					q.fileStore.UpdateTaskRetryConfig(snerdTask.GetTaskID(), fmt.Errorf("rate_limit_exceeded"))
				}
				<-q.workerPool
				continue
			}
		}

		q.execMu.Lock()

		// Double-check against the latest state in the file store to avoid TOCTOU race conditions
		if q.fileStore != nil {
			latestTask, err := q.fileStore.GetLatestTask(snerdTask.GetTaskID())
			if err == nil && latestTask != nil {
				now := time.Now().UTC()
				if (!latestTask.ExecuteAt.IsZero() && latestTask.ExecuteAt.After(now)) ||
					(!latestTask.RetryAfterTime.IsZero() && latestTask.RetryAfterTime.After(now)) {
					// Task is not due anymore
					q.execMu.Unlock()
					<-q.workerPool
					continue
				}
			} else {
				// Task was deleted
				q.execMu.Unlock()
				<-q.workerPool
				continue
			}
		}

		if q.executingTasks[snerdTask.GetTaskID()] {
			q.execMu.Unlock()
			<-q.workerPool
			continue
		}
		q.executingTasks[snerdTask.GetTaskID()] = true
		q.execMu.Unlock()

		go func(snerdTask *SnerdTask, handler func(context.Context, string) error) {
			defer func() {
				q.execMu.Lock()
				delete(q.executingTasks, snerdTask.GetTaskID())
				q.execMu.Unlock()
				<-q.workerPool
			}()

			var ctx context.Context
			var cancel context.CancelFunc
			if snerdTask.MaxExecutionSeconds != nil {
				ctx, cancel = context.WithTimeout(context.Background(), time.Duration(*snerdTask.MaxExecutionSeconds)*time.Second)
			} else {
				ctx, cancel = context.WithCancel(context.Background())
			}
			defer cancel()

			err := handler(ctx, snerdTask.Parameters)
			if err != nil {
				fmt.Println("Error executing the TASK!!!!")
				// Task failed execution
				fmt.Printf("Error executing task %s: %v\n", snerdTask.GetTaskID(), err)

				// Handle retry logic if the task has failed
				if snerdTask.RetryCount < snerdTask.MaxRetries {

					fmt.Println("RETRYING THE TASK!!!!")
					// Calculate next retry time
					snerdTask.RetryCount++

					// Update task in file store with retry information
					if q.fileStore != nil {
						fmt.Println("CALLING QUEUE FILESTORE FOR RETRYING THE TASK!!!!")
						// Convert to RetryableTask using the method on SnerdTask
						retryableTask := snerdTask.ToRetryableTask()

						// Ensure we calculate retry time properly
						retryHours := snerdTask.RetryAfterHours
						if retryHours <= 0 {
							// Default to 30 minutes if not specified
							retryHours = 0.5
						}
						retryDuration := time.Duration(retryHours * float64(time.Hour))
						retryableTask.RetryAfterTime = time.Now().Add(retryDuration)

						// Log the retry information
						fmt.Printf("Scheduling task %s for retry %d/%d after %s (at %s)\n",
							snerdTask.GetTaskID(),
							snerdTask.RetryCount,
							snerdTask.MaxRetries,
							retryDuration,
							retryableTask.RetryAfterTime.Format(time.RFC3339))

						// Update the task for retry
						updateErr := q.fileStore.UpdateTaskRetryConfig(snerdTask.GetTaskID(), err)
						if updateErr != nil {
							fmt.Printf("Error updating task retry config: %v\n", updateErr)
						} else {
							fmt.Printf("Successfully updated task %s for retry\n", snerdTask.GetTaskID())
						}
					} else {
						fmt.Printf("Warning: Cannot update task %s - no file store available\n", snerdTask.GetTaskID())
					}
				} else {
					// Max retries reached - execute the task's OnMaxRetryReached method if implemented
					fmt.Printf("Task %s reached max retries (%d)\n", snerdTask.GetTaskID(), snerdTask.MaxRetries)
					// Create a context provider function that returns the error
					contextProvider := func() interface{} {
						return err
					}
					// Pass the context provider to OnMaxRetryReached
					if callbackErr := snerdTask.OnMaxRetryReached(ctx, contextProvider); callbackErr != nil {
						fmt.Printf("Error executing OnMaxRetryReached: %v\n", callbackErr)
					}

					// Delete the task after it has reached max retries
					if q.fileStore != nil {
						// First check if the task is already deleted
						latestTask, getErr := q.fileStore.GetLatestTask(snerdTask.GetTaskID())
						if getErr != nil {
							fmt.Printf("Error getting latest task: %v\n", getErr)
						} else if latestTask.DeletedAt == nil || latestTask.DeletedAt.IsZero() {
							// Only delete if not already deleted
							deleteErr := q.fileStore.DeleteTask(snerdTask.GetTaskID())
							if deleteErr != nil {
								fmt.Printf("Error deleting task: %v\n", deleteErr)
							} else {
								if snerdTask.PayloadHash != nil {
									q.hashMu.Lock()
									delete(q.activeHashes, *snerdTask.PayloadHash)
									q.hashMu.Unlock()
								}
								fmt.Printf("Successfully deleted task %s after max retries\n", snerdTask.GetTaskID())
							}
						} else {
							fmt.Printf("Task %s is already deleted, skipping deletion\n", snerdTask.GetTaskID())
						}
					}
				}
			} else {
				// Task executed successfully
				fmt.Printf("Task %s executed successfully\n", snerdTask.GetTaskID())

				if q.fileStore != nil {
					rescheduled := false
					if snerdTask.CronExpr != nil && *snerdTask.CronExpr != "" {
						parser := cronParser()
						if sched, err := parser.Parse(*snerdTask.CronExpr); err == nil {
							snerdTask.ExecuteAt = sched.Next(time.Now().UTC())
							snerdTask.RetryCount = 0
							snerdTask.LastErrorObj = nil
							snerdTask.LastJobError = nil
							if saveErr := q.fileStore.CreateTask(snerdTask.ToRetryableTask()); saveErr != nil {
								fmt.Printf("Error rescheduling cron task: %v\n", saveErr)
							} else {
								rescheduled = true
								fmt.Printf("Cron task %s rescheduled for %s\n", snerdTask.GetTaskID(), snerdTask.ExecuteAt.Format(time.RFC3339))
							}
						}
					}

					if !rescheduled {
						fmt.Println("CALLING QUEUE FILESTORE FOR DELETING THE TASK AFTER SUCCESSFUL TASK!!!!")
						latestTask, getErr := q.fileStore.GetLatestTask(snerdTask.GetTaskID())
						if getErr != nil {
							fmt.Printf("Error getting latest task: %v\n", getErr)
						} else if latestTask.DeletedAt == nil || latestTask.DeletedAt.IsZero() {
							deleteErr := q.fileStore.DeleteTask(snerdTask.GetTaskID())
							if deleteErr != nil {
								fmt.Printf("Error deleting task %s: %v\n", snerdTask.GetTaskID(), deleteErr)
							} else {
								if snerdTask.PayloadHash != nil {
									q.hashMu.Lock()
									delete(q.activeHashes, *snerdTask.PayloadHash)
									q.hashMu.Unlock()
								}
								fmt.Printf("Successfully deleted task %s after completion\n", snerdTask.GetTaskID())
							}
						}
					}

					// Record task completion statistics
					duration := time.Since(snerdTask.CreatedAt)
					fmt.Printf("Task %s completed in %v (type=%s)\n",
						snerdTask.GetTaskID(),
						duration.Round(time.Millisecond),
						snerdTask.TaskType)
				}
			}
			atomic.AddInt64(&q.totalDequeued, 1)
		}(snerdTask, handler)
	}
}
func (q *AnyQueue) Name() string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.name
}

// Size returns the number of active tasks currently in the queue.
func (q *AnyQueue) Size() int {
	// With the new implementation, we need to count tasks from the FileStore
	if q.fileStore == nil {
		return 0
	}

	tasks, err := q.fileStore.ReadDueTasks()
	if err != nil {
		fmt.Printf("Error reading tasks: %v\n", err)
		return 0
	}

	return len(tasks)
}

// RemainingCapacity returns the number of additional tasks that can be enqueued before reaching maxSize..
func (q *AnyQueue) RemainingCapacity() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.maxSize - q.Size()
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

// SubscribeProgress returns a channel that receives real-time task progress JSON chunks.
func (q *AnyQueue) SubscribeProgress() <-chan string {
	q.progressMu.Lock()
	defer q.progressMu.Unlock()
	ch := make(chan string, 100)
	q.progressSubs = append(q.progressSubs, ch)
	return ch
}

// YieldProgress broadcasts a progress update to all subscribers.
func (q *AnyQueue) YieldProgress(taskID string, data string) {
	// Marshal to ensure special characters in data are properly escaped
	payload, err := json.Marshal(map[string]string{
		"action":  "progress",
		"task_id": taskID,
		"data":    data,
	})
	if err != nil {
		return
	}
	jsonStr := string(payload)
	q.progressMu.Lock()
	defer q.progressMu.Unlock()
	for _, ch := range q.progressSubs {
		select {
		case ch <- jsonStr:
		default:
			// Skip blocking
		}
	}
}

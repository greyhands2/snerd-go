package snerd

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
)

// CalculateDynamicQueueSize returns an optimal queue size based on system resources.
// It uses available CPU and memory to auto-scale queue capacity.
func CalculateDynamicQueueSize() int {
	numOfCPUs, _ := cpu.Counts(true)
	virtualMemory, _ := mem.VirtualMemory()

	// Use a percentage of free memory to scale queue size
	memoryFactor := int(virtualMemory.Free/(1024*1024)) / 10 // Convert to MB and divide
	return numOfCPUs * memoryFactor
}

// LogQueueUsage monitors a queue and logs its stats periodically.
// Logging stops automatically when the queue is empty or when the context is canceled.
func LogQueueUsage(ctx context.Context, queue *AnyQueue, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ctx.Done():
				log.Printf("Stopping queue monitoring for: %s", queue.Name())
				return
			case <-time.After(5 * time.Second):
				// Check if the queue is empty and stop the log function
				if queue.Size() == 0 {
					log.Printf("Queue %s is empty. Stopping queue monitoring.", queue.Name())
					return
				}

				// Log queue details
				log.Printf(
					"Queue Name: %s, Queue size: %d, Remaining capacity: %d, Total processed: %d",
					queue.Name(),
					queue.Size(),
					queue.RemainingCapacity(),
					queue.TotalProcessed(),
				)
			}
		}
	}()
}

// ProcessInMemoryQueue processes tasks in an in-memory queue with proper rate limiting
// This is specifically for NON-retryable, in-memory tasks only
func ProcessInMemoryQueue(ctx context.Context, queue *AnyQueue, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Printf("Stopping in-memory queue processor for: %s", queue.Name())
				return
			case <-ticker.C:
				// We don't need to do anything - tasks are automatically processed
				// when added to the queue via the Enqueue method, which calls processTask()

				// Just log the current state if there are tasks
				if queue.Size() > 0 {
					log.Printf("Queue %s has %d pending tasks", queue.Name(), queue.Size())
				}

				// If queue has been empty for a while, we could potentially stop
				// But for now we'll keep the processor running as long as the context is active
			}
		}
	}()
}

// ProcessRetryQueue starts a background goroutine that periodically processes retryable tasks.
// It polls for due tasks at the specified interval and processes them using the provided queue.
// The function stops when the provided context is canceled.
func ProcessRetryQueue(ctx context.Context, queue *AnyQueue, wg *sync.WaitGroup, interval time.Duration) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Check for tasks to process every 30 seconds instead of continuously
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Printf("Stopping retry queue processor for: %s", queue.Name())
				return
			case <-ticker.C:
				// Process any tasks that are due
				queue.ProcessDueTasks()
			}
		}
	}()
}

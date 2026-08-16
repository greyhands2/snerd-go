package snerd

import (
	"os"
	"testing"
	"time"
)

func TestCronRescheduling(t *testing.T) {
	// Setup queue
	q := NewAnyQueue("cron-test-queue", 100, 100*time.Millisecond)
	
	// Track executions
	executed := make(chan bool, 1)
	
	// Register handler
	RegisterTaskHandler("cron-task", func(data string) error {
		executed <- true
		return nil
	})
	
	// Create cron task
	cronStr := "* * * * * *" // every second
	task, err := NewSnerdTaskAdvanced(
		"cron-task-1",
		"cron-task",
		"{}",
		3,
		1.0,
		nil,
		nil,
		nil,
		nil,
		nil,
		&cronStr,
		nil, // webhookUrl
	)
	
	if err != nil {
		t.Fatalf("Failed to create task: %v", err)
	}
	
	// Force it to be due now for testing
	task.ExecuteAt = time.Now().UTC()
	
	// Enqueue
	err = q.Enqueue(task)
	if err != nil {
		t.Fatalf("Failed to enqueue: %v", err)
	}
	
	// Wait for execution
	select {
	case <-executed:
		// Success
	case <-time.After(3 * time.Second):
		t.Fatalf("Cron task did not execute")
	}
	
	// Wait for post-execution processing
	time.Sleep(200 * time.Millisecond)
	
	// Verify it was rescheduled, not deleted
	tasks, err := q.fileStore.ReadTasks()
	if err != nil {
		t.Fatalf("Failed to read tasks: %v", err)
	}
	
	found := false
	for _, stored := range tasks {
		if stored.TaskID == "cron-task-1" {
			if stored.DeletedAt != nil && !stored.DeletedAt.IsZero() {
				t.Fatalf("Cron task was deleted instead of rescheduled")
			}
			if stored.ExecuteAt.Before(time.Now().UTC()) {
				t.Fatalf("ExecuteAt was not advanced into the future. Current: %v", stored.ExecuteAt)
			}
			found = true
			break
		}
	}
	
	if !found {
		t.Fatalf("Cron task was not found in filestore after execution")
	}
	
	// Cleanup
	q.StopProcessor()
	os.RemoveAll("./.snerdata")
}

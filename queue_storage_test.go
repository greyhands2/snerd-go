package snerd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestNewAnyQueueWithStorage verifies that a queue created with a custom
// storage path persists tasks to that location instead of the default
// ./.snerdata/tasks/tasks.log.
func TestNewAnyQueueWithStorage(t *testing.T) {
	tmpDir := t.TempDir()
	customLog := filepath.Join(tmpDir, "custom", "tasks.log")

	var executed int32
	RegisterTaskHandler("storage_test_task", func(ctx context.Context, parameters string) error {
		atomic.AddInt32(&executed, 1)
		return nil
	})

	queue := NewAnyQueueWithStorage("storage-queue", 10, 100*time.Millisecond, customLog)
	defer queue.StopProcessor()

	if queue.tasksLogPath() != customLog {
		t.Fatalf("expected tasksLogPath %q, got %q", customLog, queue.tasksLogPath())
	}

	task, err := NewSnerdTask("storage-task-1", "storage_test_task", map[string]string{"k": "v"}, 1, 0.1)
	if err != nil {
		t.Fatalf("failed to create task: %v", err)
	}
	if err := queue.EnqueueSnerdTask(task); err != nil {
		t.Fatalf("failed to enqueue task: %v", err)
	}

	// The custom log file must exist and hold the task
	if _, err := os.Stat(customLog); err != nil {
		t.Fatalf("expected custom log file at %q: %v", customLog, err)
	}
	data, err := os.ReadFile(customLog)
	if err != nil {
		t.Fatalf("failed to read custom log: %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("custom log file is empty")
	}

	// Wait for the fast-polling processor to pick up and execute the task
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&executed) == 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if atomic.LoadInt32(&executed) != 1 {
		t.Fatalf("task was not executed from custom storage path")
	}
}

// TestNewAnyQueueWithStorageIsolation verifies that two queues on separate
// storage files only see their own tasks.
func TestNewAnyQueueWithStorageIsolation(t *testing.T) {
	tmpDir := t.TempDir()
	logA := filepath.Join(tmpDir, "queue-a", "tasks.log")
	logB := filepath.Join(tmpDir, "queue-b", "tasks.log")

	queueA := NewAnyQueueWithStorage("queue-a", 10, 1*time.Second, logA)
	defer queueA.StopProcessor()
	queueB := NewAnyQueueWithStorage("queue-b", 10, 1*time.Second, logB)
	defer queueB.StopProcessor()

	taskA, _ := NewSnerdTask("iso-task-a", "storage_test_task", map[string]string{"q": "a"}, 0, 0.1)
	if err := queueA.EnqueueSnerdTask(taskA); err != nil {
		t.Fatalf("failed to enqueue to queue A: %v", err)
	}

	if _, err := os.Stat(logA); err != nil {
		t.Fatalf("expected queue A log at %q: %v", logA, err)
	}
	if _, err := os.Stat(logB); err == nil {
		data, _ := os.ReadFile(logB)
		if len(data) > 0 {
			t.Fatalf("queue B log unexpectedly contains data: %s", data)
		}
	}
}

// TestNewAnyQueueWithStorageContention verifies that a second queue on the
// same storage file fails fast instead of silently double-executing tasks.
func TestNewAnyQueueWithStorageContention(t *testing.T) {
	tmpDir := t.TempDir()
	sharedLog := filepath.Join(tmpDir, "shared", "tasks.log")

	first := NewAnyQueueWithStorage("first-queue", 10, 1*time.Second, sharedLog)
	defer first.StopProcessor()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("expected a panic when creating a second queue on the same storage")
		}
		msg, ok := r.(string)
		if !ok || !strings.Contains(msg, "Another queue instance is already running") {
			t.Fatalf("unexpected panic message: %v", r)
		}
	}()

	_ = NewAnyQueueWithStorage("second-queue", 10, 1*time.Second, sharedLog)
}

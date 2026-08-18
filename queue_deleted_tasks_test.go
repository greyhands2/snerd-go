package snerd

import (
	"os"
	"sync"
	"testing"
	"time"
)

// TestReadDueTasksFiltersDeleted verifies that ReadDueTasks does not return
// tasks that have been soft-deleted (DeletedAt is set).
func TestReadDueTasksFiltersDeleted(t *testing.T) {
	storageDir := t.TempDir()
	fsPath := storageDir + "/tasks.log"

	fs, err := NewFileStore(fsPath)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	now := time.Now().UTC()
	past := now.Add(-1 * time.Hour)

	// Create 3 due tasks
	for i := 0; i < 3; i++ {
		task := &RetryableTask{
			TaskID:         "task-" + string(rune('A'+i)),
			TaskType:       "test",
			ExecuteAt:      past,
			RetryAfterTime: past,
		}
		if err := fs.CreateTask(task); err != nil {
			t.Fatalf("CreateTask: %v", err)
		}
	}

	// Verify all 3 are returned
	due, err := fs.ReadDueTasks()
	if err != nil {
		t.Fatalf("ReadDueTasks: %v", err)
	}
	if len(due) != 3 {
		t.Fatalf("expected 3 due tasks, got %d", len(due))
	}

	// Delete one task
	if err := fs.DeleteTask("task-A"); err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}

	// Verify only 2 are returned (deleted one is filtered)
	due, err = fs.ReadDueTasks()
	if err != nil {
		t.Fatalf("ReadDueTasks after delete: %v", err)
	}
	if len(due) != 2 {
		t.Fatalf("expected 2 due tasks after delete, got %d", len(due))
	}

	// Verify the deleted task is NOT in the results
	for _, task := range due {
		if task.TaskID == "task-A" {
			t.Fatal("deleted task-A should not be returned by ReadDueTasks")
		}
	}
}

// TestReadDueTasksConcurrentDeleteStress exercises ReadDueTasks under
// concurrent enqueue + delete + read pressure to ensure no race conditions.
func TestReadDueTasksConcurrentDeleteStress(t *testing.T) {
	storageDir := t.TempDir()
	fsPath := storageDir + "/tasks.log"

	fs, err := NewFileStore(fsPath)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	past := time.Now().UTC().Add(-1 * time.Hour)
	const numTasks = 200

	// Pre-populate tasks
	for i := 0; i < numTasks; i++ {
		task := &RetryableTask{
			TaskID:         "stress-" + string(rune(i/256)) + string(rune(i%256)),
			TaskType:       "test",
			ExecuteAt:      past,
			RetryAfterTime: past,
		}
		if err := fs.CreateTask(task); err != nil {
			t.Fatalf("CreateTask %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup

	// Goroutine 1: continuously delete tasks
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numTasks; i += 2 {
			taskID := "stress-" + string(rune(i/256)) + string(rune(i%256))
			_ = fs.DeleteTask(taskID)
		}
	}()

	// Goroutine 2: continuously read due tasks
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 50; j++ {
			due, err := fs.ReadDueTasks()
			if err != nil {
				t.Errorf("ReadDueTasks: %v", err)
				return
			}
			// Verify no deleted tasks are returned
			for _, task := range due {
				if task.DeletedAt != nil && !task.DeletedAt.IsZero() {
					t.Errorf("ReadDueTasks returned a deleted task: %s", task.TaskID)
					return
				}
			}
		}
	}()

	wg.Wait()

	// Final check: read due tasks and verify none are deleted
	due, err := fs.ReadDueTasks()
	if err != nil {
		t.Fatalf("final ReadDueTasks: %v", err)
	}
	for _, task := range due {
		if task.DeletedAt != nil && !task.DeletedAt.IsZero() {
			t.Fatalf("deleted task %s leaked through ReadDueTasks", task.TaskID)
		}
	}

	t.Logf("Final due tasks: %d (from %d original, half deleted)", len(due), numTasks)
}

// TestReadDueTasksDoesNotReturnFutureTasks ensures the time filter still works
// alongside the new DeletedAt filter.
func TestReadDueTasksDoesNotReturnFutureTasks(t *testing.T) {
	storageDir := t.TempDir()
	fsPath := storageDir + "/tasks.log"

	fs, err := NewFileStore(fsPath)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}

	past := time.Now().UTC().Add(-1 * time.Hour)
	future := time.Now().UTC().Add(1 * time.Hour)

	// One due task, one future task
	if err := fs.CreateTask(&RetryableTask{
		TaskID: "due-task", TaskType: "test", ExecuteAt: past, RetryAfterTime: past,
	}); err != nil {
		t.Fatal(err)
	}
	if err := fs.CreateTask(&RetryableTask{
		TaskID: "future-task", TaskType: "test", ExecuteAt: future, RetryAfterTime: past,
	}); err != nil {
		t.Fatal(err)
	}

	due, err := fs.ReadDueTasks()
	if err != nil {
		t.Fatalf("ReadDueTasks: %v", err)
	}
	if len(due) != 1 {
		t.Fatalf("expected 1 due task, got %d", len(due))
	}
	if due[0].TaskID != "due-task" {
		t.Fatalf("expected due-task, got %s", due[0].TaskID)
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

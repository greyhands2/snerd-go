package snerd

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/gofrs/flock"
)

type RateLimitEntry struct {
	Count      int       `json:"count"`
	WindowEnd  time.Time `json:"window_end"`
}

type RateLimiter struct {
	lockFilePath string
	dataFilePath string
}

func NewRateLimiter(storageDir string) *RateLimiter {
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		fmt.Printf("[RateLimiter] Error creating storage dir: %v\n", err)
	}
	return &RateLimiter{
		lockFilePath: filepath.Join(storageDir, "rate_limits.lock"),
		dataFilePath: filepath.Join(storageDir, "rate_limits.json"),
	}
}

// CheckLimit returns true if the task is allowed to execute, false if it should be rate-limited
func (r *RateLimiter) CheckLimit(group string, maxPerMinute int) bool {
	if maxPerMinute <= 0 {
		return true // No limit
	}

	fileLock := flock.New(r.lockFilePath)
	
	// Wait up to 5 seconds to acquire lock
	locked := false
	for i := 0; i < 50; i++ {
		success, err := fileLock.TryLock()
		if err == nil && success {
			locked = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !locked {
		fmt.Printf("[RateLimiter] Warning: could not acquire lock for %s, allowing execution\n", group)
		return true
	}
	defer fileLock.Unlock()

	// Read current limits
	limits := make(map[string]RateLimitEntry)
	if data, err := ioutil.ReadFile(r.dataFilePath); err == nil && len(data) > 0 {
		if err := json.Unmarshal(data, &limits); err != nil {
			fmt.Printf("[RateLimiter] Warning: malformed rate_limits.json, resetting\n")
		}
	}

	now := time.Now()
	entry, exists := limits[group]

	// Reset if window has expired
	if !exists || now.After(entry.WindowEnd) {
		entry = RateLimitEntry{
			Count:     0,
			WindowEnd: now.Add(60 * time.Second),
		}
	}

	if entry.Count >= maxPerMinute {
		// Rate limit exceeded
		return false
	}

	// Approve execution and increment count
	entry.Count++
	limits[group] = entry

	// Save back to file
	if data, err := json.Marshal(limits); err == nil {
		ioutil.WriteFile(r.dataFilePath, data, 0644)
	}

	return true
}

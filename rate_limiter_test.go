package snerd

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRateLimiter(t *testing.T) {
	tempDir := t.TempDir()
	rl := NewRateLimiter(tempDir)

	group := "test-api"
	maxPerMinute := 2

	// First two should succeed
	assert.True(t, rl.CheckLimit(group, maxPerMinute))
	assert.True(t, rl.CheckLimit(group, maxPerMinute))

	// Third should fail
	assert.False(t, rl.CheckLimit(group, maxPerMinute))

	// Simulate window expiration by manually modifying the file
	dataFilePath := filepath.Join(tempDir, "rate_limits.json")
	
	// Fast forward time to the past
	pastTime := time.Now().Add(-61 * time.Second)
	
	// We'll just overwrite it to simulate expiry
	os.WriteFile(dataFilePath, []byte(fmt.Sprintf(`{"%s":{"count":2,"window_end":"%s"}}`, group, pastTime.Format(time.RFC3339))), 0644)

	// Now it should succeed again
	assert.True(t, rl.CheckLimit(group, maxPerMinute))
}

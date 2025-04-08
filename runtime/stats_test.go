package runtime

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStats(t *testing.T) {
	sc := NewStatsCollector()
	sc.RecordCronTask("make_foos", 10*time.Second)
	sc.RecordCronTask("make_foos", 5*time.Second)
	sc.RecordLLMCall("openai", 7*time.Second)
	sc.RecordLLMCall("openai", 3*time.Second)
	sc.RecordLLMCall("anthropic", 4*time.Second)

	stats := sc.Extract()
	assert.Equal(t, 2, stats.CronTaskCount["make_foos"])
	assert.Equal(t, 15*time.Second, stats.CronTaskDuration["make_foos"])
	assert.Equal(t, 2, stats.LLMCallCount["openai"])
	assert.Equal(t, 10*time.Second, stats.LLMCallDuration["openai"])
	assert.Equal(t, 1, stats.LLMCallCount["anthropic"])
	assert.Equal(t, 4*time.Second, stats.LLMCallDuration["anthropic"])

	datums := stats.ToMetrics()
	assert.Len(t, datums, 8)
}

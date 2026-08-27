package relay

import "time"

const adaptiveThroughputWeight = 0.25

// batchController 为单个 output 保存自适应状态。状态无需持久化，重启后会从安全值重新学习。
type batchController struct {
	enabled        bool
	minimum        int
	maximum        int
	current        int
	target         time.Duration
	ewmaThroughput float64
}

func newBatchController(backend *httpBackend) *batchController {
	current := backend.maxBatch
	if backend.adaptive {
		current = DefaultBatchSizeKB * KB
		current = clampBatch(current, backend.minBatch, backend.maxBatch)
	}
	return &batchController{
		enabled: backend.adaptive,
		minimum: backend.minBatch,
		maximum: backend.maxBatch,
		current: current,
		target:  backend.targetBatch,
	}
}

func (c *batchController) limit() int {
	return c.current
}

func (c *batchController) success(bytes int, duration time.Duration) {
	if !c.enabled || bytes < c.minimum || duration <= 0 {
		return
	}

	sample := float64(bytes) / duration.Seconds()
	if c.ewmaThroughput == 0 {
		c.ewmaThroughput = sample
	} else {
		c.ewmaThroughput = adaptiveThroughputWeight*sample +
			(1-adaptiveThroughputWeight)*c.ewmaThroughput
	}
	desired := clampBatch(int(c.ewmaThroughput*c.target.Seconds()), c.minimum, c.maximum)
	if desired < c.current {
		// 慢速成功时最多减半；网络错误由 failure 立即执行减半。
		minimumNext := c.current / 2
		if desired < minimumNext {
			desired = minimumNext
		}
		c.current = clampBatch(desired, c.minimum, c.maximum)
		return
	}
	if desired > c.current {
		// 恢复时每次最多增长 25%，避免短暂好转造成大批量震荡。
		step := c.current / 4
		if step < KB {
			step = KB
		}
		if desired > c.current+step {
			desired = c.current + step
		}
		c.current = clampBatch(desired, c.minimum, c.maximum)
	}
}

func (c *batchController) failure() {
	if !c.enabled {
		return
	}
	c.current = clampBatch(c.current/2, c.minimum, c.maximum)
	// 丢弃旧吞吐估计，让恢复后的首次有效写入重新反映当前链路。
	c.ewmaThroughput = 0
}

func clampBatch(value, minimum, maximum int) int {
	if value < minimum {
		return minimum
	}
	if value > maximum {
		return maximum
	}
	return value
}

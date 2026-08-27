package relay

import (
	"strings"
	"testing"
	"time"
)

func TestAdaptiveBatchControllerRespondsToLinkSpeed(t *testing.T) {
	controller := newBatchController(&httpBackend{
		maxBatch:    2048 * KB,
		minBatch:    128 * KB,
		adaptive:    true,
		targetBatch: 5 * time.Second,
	})
	if got, want := controller.limit(), 512*KB; got != want {
		t.Fatalf("初始批量为 %d，期望 %d", got, want)
	}

	controller.success(512*KB, 20*time.Second)
	if got, want := controller.limit(), 256*KB; got != want {
		t.Fatalf("慢速成功后批量为 %d，期望最多减半到 %d", got, want)
	}

	controller.failure()
	if got, want := controller.limit(), 128*KB; got != want {
		t.Fatalf("可重试失败后批量为 %d，期望减半到下限 %d", got, want)
	}

	controller.success(128*KB, time.Second)
	if got, want := controller.limit(), 160*KB; got != want {
		t.Fatalf("链路恢复后批量为 %d，期望按 25%% 增长到 %d", got, want)
	}
}

func TestAdaptiveBatchControllerIgnoresTinySamples(t *testing.T) {
	controller := newBatchController(&httpBackend{
		maxBatch:    512 * KB,
		minBatch:    128 * KB,
		adaptive:    true,
		targetBatch: 5 * time.Second,
	})
	controller.success(50, time.Second)
	if got, want := controller.limit(), 512*KB; got != want {
		t.Fatalf("零散小请求不应改变批量，实际为 %d，期望 %d", got, want)
	}
}

func TestStaticBatchControllerDoesNotAdjust(t *testing.T) {
	controller := newBatchController(&httpBackend{
		maxBatch:    256 * KB,
		minBatch:    128 * KB,
		targetBatch: 5 * time.Second,
	})
	controller.failure()
	controller.success(256*KB, 30*time.Second)
	if got, want := controller.limit(), 256*KB; got != want {
		t.Fatalf("未启用自适应时批量发生变化：%d，期望 %d", got, want)
	}
}

func TestAdaptiveBatchConfiguration(t *testing.T) {
	backend, err := newHTTPBackend(&HTTPOutputConfig{
		Name:                "slow-link",
		Location:            "http://127.0.0.1:8086/write",
		MaxBatchKB:          2048,
		AdaptiveBatch:       true,
		MinBatchKB:          128,
		TargetBatchDuration: "5s",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !backend.adaptive || backend.minBatch != 128*KB || backend.maxBatch != 2048*KB || backend.targetBatch != 5*time.Second {
		t.Fatalf("自适应配置解析错误：%#v", backend)
	}
}

func TestAdaptiveBatchConfigurationRejectsInvalidRange(t *testing.T) {
	_, err := newHTTPBackend(&HTTPOutputConfig{
		Location:   "http://127.0.0.1:8086/write",
		MaxBatchKB: 128,
		MinBatchKB: 256,
	})
	if err == nil || !strings.Contains(err.Error(), "min-batch-kb") {
		t.Fatalf("应拒绝批量下限大于上限的配置，实际错误：%v", err)
	}
}

func TestAdaptiveBatchConfigurationRejectsInvalidDuration(t *testing.T) {
	_, err := newHTTPBackend(&HTTPOutputConfig{
		Location:            "http://127.0.0.1:8086/write",
		TargetBatchDuration: "0s",
	})
	if err == nil || !strings.Contains(err.Error(), "target-batch-duration") {
		t.Fatalf("应拒绝非正数目标耗时，实际错误：%v", err)
	}
}

func TestAdaptiveBatchConfigurationRequiresTargetBelowTimeout(t *testing.T) {
	_, err := newHTTPBackend(&HTTPOutputConfig{
		Location:            "http://127.0.0.1:8086/write",
		Timeout:             "5s",
		AdaptiveBatch:       true,
		TargetBatchDuration: "5s",
	})
	if err == nil || !strings.Contains(err.Error(), "必须小于 timeout") {
		t.Fatalf("应拒绝不小于请求超时的目标耗时，实际错误：%v", err)
	}
}

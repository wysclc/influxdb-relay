package relay

import "testing"

func TestLoadDurableQueueSample(t *testing.T) {
	cfg, err := LoadConfigFile("../sample_buffered.toml")
	if err != nil {
		t.Fatalf("加载持久化队列示例失败: %v", err)
	}
	if len(cfg.HTTPRelays) != 1 {
		t.Fatalf("HTTP relay 数量为 %d，期望 1", len(cfg.HTTPRelays))
	}

	httpRelay := cfg.HTTPRelays[0]
	if httpRelay.QueuePath != "/var/lib/influxdb-relay/example-http.queue.db" {
		t.Fatalf("queue-path 解析错误: %q", httpRelay.QueuePath)
	}
	if len(httpRelay.Outputs) != 2 {
		t.Fatalf("output 数量为 %d，期望 2", len(httpRelay.Outputs))
	}
	for _, output := range httpRelay.Outputs {
		if output.BufferSizeMB <= 0 {
			t.Fatalf("output %q 没有启用持久化队列", output.Name)
		}
	}
}

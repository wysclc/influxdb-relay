package relay

import (
	"context"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

type controlledPoster struct {
	started chan struct{}
	release <-chan struct{}
	status  int
	err     error
	calls   int32
}

func (p *controlledPoster) post(ctx context.Context, _ []byte, _, _ string) (*responseData, error) {
	atomic.AddInt32(&p.calls, 1)
	if p.started != nil {
		select {
		case p.started <- struct{}{}:
		default:
		}
	}
	if p.release != nil {
		select {
		case <-p.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if p.err != nil {
		return nil, p.err
	}
	return &responseData{StatusCode: p.status}, nil
}

func TestDurableQueueIsolatesFailedOutput(t *testing.T) {
	badRelease := make(chan struct{})
	badStarted := make(chan struct{}, 1)
	goodStarted := make(chan struct{}, 1)
	bad := testBackend("bad", &controlledPoster{
		started: badStarted,
		release: badRelease,
		status:  500,
	}, 1<<20)
	good := testBackend("good", &controlledPoster{
		started: goodStarted,
		status:  204,
	}, 1<<20)

	queue, cleanup := openTestQueue(t, bad, good)
	defer cleanup()
	if _, err := queue.enqueue([]byte("cpu value=1i 1\n"), "db=test", ""); err != nil {
		t.Fatalf("入队失败: %v", err)
	}

	waitSignal(t, badStarted, "故障 output 没有开始投递")
	waitSignal(t, goodStarted, "正常 output 被故障 output 阻塞")
	waitFor(t, func() bool {
		active, _ := queueBytes(t, queue, "good")
		return active == 0
	}, "正常 output 的记录没有独立确认")

	badActive, _ := queueBytes(t, queue, "bad")
	if badActive == 0 {
		t.Fatal("故障 output 的未确认记录被意外删除")
	}

	close(badRelease)
	if err := queue.Close(); err != nil {
		t.Fatalf("关闭队列失败: %v", err)
	}
}

func TestDurableQueueSkipsOnlyFullOutput(t *testing.T) {
	payload, err := encodeRecord(durableRecord{query: "db=test", body: []byte("cpu value=1i 1\n")})
	if err != nil {
		t.Fatal(err)
	}
	limit := int64(8 + len(payload))
	release := make(chan struct{})
	firstStarted := make(chan struct{}, 1)
	secondStarted := make(chan struct{}, 1)
	first := testBackend("first", &controlledPoster{started: firstStarted, release: release, status: 204}, limit)
	second := testBackend("second", &controlledPoster{started: secondStarted, release: release, status: 204}, limit*2)
	queue, cleanup := openTestQueue(t, first, second)
	defer cleanup()

	if _, err := queue.enqueue([]byte("cpu value=1i 1\n"), "db=test", ""); err != nil {
		t.Fatalf("第一次入队失败: %v", err)
	}
	waitSignal(t, firstStarted, "first output 没有读取记录")
	waitSignal(t, secondStarted, "second output 没有读取记录")

	result, err := queue.enqueue([]byte("cpu value=2i 2\n"), "db=test", "")
	if err != nil {
		t.Fatalf("仍有 output 可接收时不应返回错误: %v", err)
	}
	if len(result.accepted) != 1 || result.accepted[0] != "second" {
		t.Fatalf("第二次写入的接收 output 错误: %#v", result.accepted)
	}
	if len(result.full) != 1 || result.full[0] != "first" {
		t.Fatalf("第二次写入的满队列 output 错误: %#v", result.full)
	}
	firstActive, _ := queueBytes(t, queue, "first")
	secondActive, _ := queueBytes(t, queue, "second")
	if firstActive != limit || secondActive != limit*2 {
		t.Fatalf("各 output 入队结果错误: first=%d second=%d", firstActive, secondActive)
	}

	result, err = queue.enqueue([]byte("cpu value=3i 3\n"), "db=test", "")
	if !errors.Is(err, ErrBufferFull) {
		t.Fatalf("所有队列都满时应返回 ErrBufferFull，实际为 %v", err)
	}
	if len(result.accepted) != 0 || len(result.full) != 2 {
		t.Fatalf("所有队列已满的结果错误: %#v", result)
	}

	close(release)
	if err := queue.Close(); err != nil {
		t.Fatalf("关闭队列失败: %v", err)
	}
}

func TestDurableQueueRecoversPendingRecordsAfterRestart(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "influxdb-relay-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)
	path := filepath.Join(tempDir, "restart.db")
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	failing := testBackend("output", &controlledPoster{
		started: started,
		release: release,
		err:     errors.New("backend unavailable"),
	}, 1<<20)
	queue, err := newDurableQueue(path, []*httpBackend{failing})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.enqueue([]byte("cpu value=1i 1\n"), "db=test", "token"); err != nil {
		t.Fatal(err)
	}
	waitSignal(t, started, "首次投递没有开始")

	closed := make(chan error, 1)
	go func() { closed <- queue.Close() }()
	<-queue.stop
	close(release)
	if err := <-closed; err != nil {
		t.Fatalf("关闭首次队列失败: %v", err)
	}

	recovered := make(chan struct{}, 1)
	working := testBackend("output", &controlledPoster{started: recovered, status: 204}, 1<<20)
	queue, err = newDurableQueue(path, []*httpBackend{working})
	if err != nil {
		t.Fatalf("重新打开队列失败: %v", err)
	}
	waitSignal(t, recovered, "重启后没有恢复投递")
	waitFor(t, func() bool {
		active, _ := queueBytes(t, queue, "output")
		return active == 0
	}, "恢复的记录没有确认")
	if err := queue.Close(); err != nil {
		t.Fatalf("关闭恢复队列失败: %v", err)
	}
}

func TestDurableQueueMovesNonRetryableResponseToDeadLetter(t *testing.T) {
	poster := &controlledPoster{status: 400}
	backend := testBackend("invalid", poster, 1<<20)
	queue, cleanup := openTestQueue(t, backend)
	defer cleanup()
	if _, err := queue.enqueue([]byte("cpu value=1i 1\n"), "db=test", ""); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool {
		active, dead := queueBytes(t, queue, "invalid")
		return active == 0 && dead > 0
	}, "不可重试记录没有进入 dead-letter")
	time.Sleep(30 * time.Millisecond)
	if calls := atomic.LoadInt32(&poster.calls); calls != 1 {
		t.Fatalf("不可重试的 400 响应被重复投递了 %d 次", calls)
	}
	if err := queue.Close(); err != nil {
		t.Fatalf("关闭队列失败: %v", err)
	}
}

func TestResponseFailureIncludesBackendBody(t *testing.T) {
	reason := responseFailure(&responseData{
		StatusCode: http.StatusServiceUnavailable,
		Body:       []byte(`{"error":"disk full"}`),
	})
	if !strings.Contains(reason, "HTTP 503") || !strings.Contains(reason, "disk full") {
		t.Fatalf("后端错误原因不完整: %q", reason)
	}
}

func TestDurableHTTPAcknowledgesWhenAtLeastOneOutputAccepts(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	fullPoster := &controlledPoster{status: http.StatusNoContent}
	full := testBackend("full", fullPoster, 1)
	slow := testBackend("slow", &controlledPoster{
		started: started,
		release: release,
		status:  http.StatusServiceUnavailable,
	}, 1<<20)
	queue, cleanup := openTestQueue(t, full, slow)
	defer cleanup()
	relay := &HTTP{
		name:     "durable-http",
		schema:   "http",
		backends: []*httpBackend{full, slow},
		queue:    queue,
		stopped:  make(chan struct{}),
	}

	request := httptest.NewRequest(http.MethodPost, "/write?db=test&precision=n", strings.NewReader("cpu value=1i 1"))
	request.Header.Set("Authorization", "Token secret")
	response := httptest.NewRecorder()
	relay.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("至少一个 output 入队后应返回 204，实际为 %d: %s", response.Code, response.Body.String())
	}
	waitSignal(t, started, "WAL 提交后 worker 没有开始异步投递")
	if calls := atomic.LoadInt32(&fullPoster.calls); calls != 0 {
		t.Fatalf("已满 output 不应收到投递，实际调用 %d 次", calls)
	}
	fullActive, _ := queueBytes(t, queue, "full")
	if fullActive != 0 {
		t.Fatalf("已满 output 不应新增记录，active=%d", fullActive)
	}
	active, _ := queueBytes(t, queue, "slow")
	if active == 0 {
		t.Fatal("远端尚未成功时 WAL 记录被意外删除")
	}

	close(release)
	if err := queue.Close(); err != nil {
		t.Fatalf("关闭队列失败: %v", err)
	}
}

func TestDurableHTTPReturns503OnlyWhenAllOutputsAreFull(t *testing.T) {
	poster := &controlledPoster{status: http.StatusNoContent}
	backend := testBackend("full", poster, 1)
	queue, cleanup := openTestQueue(t, backend)
	defer cleanup()
	relay := &HTTP{
		name:     "durable-http",
		schema:   "http",
		backends: []*httpBackend{backend},
		queue:    queue,
		stopped:  make(chan struct{}),
	}

	request := httptest.NewRequest(http.MethodPost, "/write?db=test&precision=n", strings.NewReader("cpu value=1i 1"))
	response := httptest.NewRecorder()
	relay.ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("所有 output 都满时应返回 503，实际为 %d", response.Code)
	}
	if response.Header().Get("Retry-After") != "1" {
		t.Fatalf("所有 output 都满时缺少 Retry-After: %q", response.Header().Get("Retry-After"))
	}
	if !strings.Contains(response.Body.String(), "所有 output") {
		t.Fatalf("错误信息没有说明所有 output 均已满: %s", response.Body.String())
	}
	if calls := atomic.LoadInt32(&poster.calls); calls != 0 {
		t.Fatalf("已满 output 不应收到投递，实际调用 %d 次", calls)
	}
}

func testBackend(name string, p poster, limit int64) *httpBackend {
	return &httpBackend{
		poster:      p,
		name:        name,
		maxBuffered: limit,
		maxBatch:    1 << 20,
		maxDelay:    10 * time.Millisecond,
	}
}

func openTestQueue(t *testing.T, backends ...*httpBackend) (*durableQueue, func()) {
	t.Helper()
	tempDir, err := ioutil.TempDir("", "influxdb-relay-test-")
	if err != nil {
		t.Fatal(err)
	}
	queue, err := newDurableQueue(filepath.Join(tempDir, "queue.db"), backends)
	if err != nil {
		_ = os.RemoveAll(tempDir)
		t.Fatal(err)
	}
	cleanup := func() {
		_ = queue.Close()
		_ = os.RemoveAll(tempDir)
	}
	return queue, cleanup
}

func queueBytes(t *testing.T, queue *durableQueue, name string) (int64, int64) {
	t.Helper()
	var active, dead int64
	if err := queue.db.View(func(tx *bolt.Tx) error {
		output := tx.Bucket(queueRootBucket).Bucket([]byte(name))
		meta := output.Bucket(metaBucket)
		active = readCounter(meta, activeBytesKey)
		dead = readCounter(meta, deadBytesKey)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return active, dead
}

func waitSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal(message)
	}
}

func waitFor(t *testing.T, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(message)
}

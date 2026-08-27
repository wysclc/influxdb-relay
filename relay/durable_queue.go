package relay

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	queueInitialRetry = 500 * time.Millisecond
	queueOpenTimeout  = 3 * time.Second
)

var (
	queueRootBucket = []byte("outputs")
	pendingBucket   = []byte("pending")
	deadBucket      = []byte("dead")
	metaBucket      = []byte("meta")
	activeBytesKey  = []byte("active-bytes")
	deadBytesKey    = []byte("dead-bytes")
)

type durableRecord struct {
	query string
	auth  string
	body  []byte
}

type durableBatch struct {
	ids   []uint64
	query string
	auth  string
	body  []byte
}

type enqueueResult struct {
	evicted []evictedOutput
}

type evictedOutput struct {
	name    string
	records int
	bytes   int64
}

// durableQueue 在一个 BoltDB 事务中把最新请求写入所有 output。
// 空间不足时淘汰最老的待投递记录，每个 output 始终保留最新数据。
type durableQueue struct {
	db       *bolt.DB
	path     string
	backends []*httpBackend
	wakes    map[string]chan struct{}
	stop     chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
	close    sync.Once
	wg       sync.WaitGroup
}

func newDurableQueue(path string, backends []*httpBackend) (*durableQueue, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return nil, fmt.Errorf("创建持久化队列目录失败: %v", err)
	}

	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: queueOpenTimeout})
	if err != nil {
		return nil, fmt.Errorf("打开持久化队列 %q 失败: %v", path, err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("设置持久化队列 %q 权限失败: %v", path, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	q := &durableQueue{
		db:       db,
		path:     path,
		backends: backends,
		wakes:    make(map[string]chan struct{}, len(backends)),
		stop:     make(chan struct{}),
		ctx:      ctx,
		cancel:   cancel,
	}

	if err := q.db.Update(func(tx *bolt.Tx) error {
		root, err := tx.CreateBucketIfNotExists(queueRootBucket)
		if err != nil {
			return err
		}
		for _, backend := range backends {
			output, err := root.CreateBucketIfNotExists([]byte(backend.name))
			if err != nil {
				return err
			}
			if _, err = output.CreateBucketIfNotExists(pendingBucket); err != nil {
				return err
			}
			if _, err = output.CreateBucketIfNotExists(deadBucket); err != nil {
				return err
			}
			if _, err = output.CreateBucketIfNotExists(metaBucket); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		cancel()
		_ = db.Close()
		return nil, fmt.Errorf("初始化持久化队列 %q 失败: %v", path, err)
	}

	for _, backend := range backends {
		q.wakes[backend.name] = make(chan struct{}, 1)
	}
	for _, backend := range backends {
		q.wg.Add(1)
		go q.runBackend(backend)
	}

	return q, nil
}

func (q *durableQueue) enqueue(body []byte, query, auth string) (enqueueResult, error) {
	var result enqueueResult
	payload, err := encodeRecord(durableRecord{query: query, auth: auth, body: body})
	if err != nil {
		return result, err
	}
	recordBytes := int64(8 + len(payload))

	err = q.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(queueRootBucket)

		// 先为每个 output 淘汰最老记录，再写入最新记录。
		for _, backend := range q.backends {
			output := root.Bucket([]byte(backend.name))
			pending := output.Bucket(pendingBucket)
			meta := output.Bucket(metaBucket)
			active := readCounter(meta, activeBytesKey)
			evicted := evictedOutput{name: backend.name}

			cursor := pending.Cursor()
			for active+recordBytes > backend.maxBuffered {
				key, value := cursor.First()
				if key == nil {
					active = 0
					break
				}
				size := int64(len(key) + len(value))
				if err := cursor.Delete(); err != nil {
					return err
				}
				active -= size
				evicted.records++
				evicted.bytes += size
			}
			if active < 0 {
				active = 0
			}

			sequence, err := pending.NextSequence()
			if err != nil {
				return err
			}
			if err := pending.Put(sequenceKey(sequence), payload); err != nil {
				return err
			}
			if err := writeCounter(meta, activeBytesKey, active+recordBytes); err != nil {
				return err
			}
			if evicted.records > 0 {
				result.evicted = append(result.evicted, evicted)
			}
		}
		return nil
	})
	if err != nil {
		return enqueueResult{}, err
	}
	for _, backend := range q.backends {
		q.wake(backend.name)
	}
	return result, nil
}

func (q *durableQueue) runBackend(backend *httpBackend) {
	defer q.wg.Done()
	batchControl := newBatchController(backend)
	if batchControl.enabled {
		log.Printf("output %q 启用自适应批量（initial=%dKB, min=%dKB, max=%dKB, target=%v）",
			backend.name, batchControl.limit()/KB, batchControl.minimum/KB, batchControl.maximum/KB, batchControl.target)
	}

	interval := queueInitialRetry
	if backend.maxDelay < interval {
		interval = backend.maxDelay
	}

	for {
		select {
		case <-q.stop:
			return
		default:
		}

		batch, found, err := q.peekBatch(backend, batchControl.limit())
		if err != nil {
			log.Printf("读取 output %q 的持久化队列失败: %v", backend.name, err)
			if !q.waitRetry(interval) {
				return
			}
			interval = nextRetry(interval, backend.maxDelay)
			continue
		}
		if !found {
			interval = queueInitialRetry
			if interval > backend.maxDelay {
				interval = backend.maxDelay
			}
			select {
			case <-q.stop:
				return
			case <-q.wakes[backend.name]:
				continue
			}
		}

		attemptStarted := time.Now()
		resp, postErr := backend.poster.post(q.ctx, batch.body, batch.query, batch.auth)
		attemptDuration := time.Since(attemptStarted)
		if postErr != nil {
			select {
			case <-q.stop:
				return
			default:
			}
		}
		if postErr == nil && resp != nil && resp.StatusCode/100 == 2 {
			if err := q.ack(backend, batch.ids); err != nil {
				log.Printf("确认 output %q 的持久化记录失败，将安全重放: %v", backend.name, err)
				if !q.waitRetry(interval) {
					return
				}
				interval = nextRetry(interval, backend.maxDelay)
				continue
			}
			batchControl.success(len(batch.body), attemptDuration)
			log.Printf("output %q HTTP 写入成功（status=%d, records=%d, bytes=%d, duration=%v, batch-limit=%dKB）",
				backend.name, resp.StatusCode, len(batch.ids), len(batch.body), attemptDuration, batchControl.limit()/KB)
			interval = queueInitialRetry
			if interval > backend.maxDelay {
				interval = backend.maxDelay
			}
			continue
		}

		if postErr == nil && resp != nil && !isRetryableStatus(resp.StatusCode) {
			reason := responseFailure(resp)
			evicted, err := q.moveToDead(backend, batch.ids, reason)
			if err != nil {
				log.Printf("移动 output %q 的不可重试记录到 dead-letter 失败: %v", backend.name, err)
				if !q.waitRetry(interval) {
					return
				}
				interval = nextRetry(interval, backend.maxDelay)
				continue
			}
			log.Printf("output %q HTTP 写入失败且不可重试，已将记录移入 dead-letter（records=%d, bytes=%d, duration=%v）: %s",
				backend.name, len(batch.ids), len(batch.body), attemptDuration, reason)
			if evicted > 0 {
				log.Printf("output %q 的 dead-letter 达到上限，淘汰了 %d 条最旧记录", backend.name, evicted)
			}
			interval = queueInitialRetry
			if interval > backend.maxDelay {
				interval = backend.maxDelay
			}
			continue
		}

		batchControl.failure()
		if postErr != nil {
			log.Printf("output %q HTTP 写入失败，将重试（records=%d, bytes=%d, duration=%v, batch-limit=%dKB）: %v",
				backend.name, len(batch.ids), len(batch.body), attemptDuration, batchControl.limit()/KB, postErr)
		} else if resp != nil {
			log.Printf("output %q HTTP 写入失败，将重试（records=%d, bytes=%d, duration=%v, batch-limit=%dKB）: %s",
				backend.name, len(batch.ids), len(batch.body), attemptDuration, batchControl.limit()/KB, responseFailure(resp))
		} else {
			log.Printf("output %q HTTP 写入失败，将重试（records=%d, bytes=%d, duration=%v, batch-limit=%dKB）: 未收到有效响应",
				backend.name, len(batch.ids), len(batch.body), attemptDuration, batchControl.limit()/KB)
		}

		if !q.waitRetry(interval) {
			return
		}
		interval = nextRetry(interval, backend.maxDelay)
	}
}

func (q *durableQueue) peekBatch(backend *httpBackend, batchLimit int) (durableBatch, bool, error) {
	var result durableBatch
	err := q.db.View(func(tx *bolt.Tx) error {
		output := tx.Bucket(queueRootBucket).Bucket([]byte(backend.name))
		cursor := output.Bucket(pendingBucket).Cursor()

		for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
			record, err := decodeRecord(value)
			if err != nil {
				return fmt.Errorf("记录 %x 损坏: %v", key, err)
			}
			if len(result.ids) > 0 {
				if record.query != result.query || record.auth != result.auth {
					break
				}
				if len(result.body)+len(record.body) > batchLimit {
					break
				}
			} else {
				result.query = record.query
				result.auth = record.auth
			}

			result.ids = append(result.ids, binary.BigEndian.Uint64(key))
			result.body = append(result.body, record.body...)
		}
		return nil
	})
	if err != nil {
		return durableBatch{}, false, err
	}
	return result, len(result.ids) > 0, nil
}

func (q *durableQueue) ack(backend *httpBackend, ids []uint64) error {
	return q.db.Update(func(tx *bolt.Tx) error {
		output := tx.Bucket(queueRootBucket).Bucket([]byte(backend.name))
		pending := output.Bucket(pendingBucket)
		meta := output.Bucket(metaBucket)
		active := readCounter(meta, activeBytesKey)

		for _, id := range ids {
			key := sequenceKey(id)
			value := pending.Get(key)
			if value == nil {
				continue
			}
			active -= int64(len(key) + len(value))
			if err := pending.Delete(key); err != nil {
				return err
			}
		}
		if active < 0 {
			active = 0
		}
		return writeCounter(meta, activeBytesKey, active)
	})
}

func (q *durableQueue) moveToDead(backend *httpBackend, ids []uint64, reason string) (int, error) {
	evicted := 0
	err := q.db.Update(func(tx *bolt.Tx) error {
		output := tx.Bucket(queueRootBucket).Bucket([]byte(backend.name))
		pending := output.Bucket(pendingBucket)
		dead := output.Bucket(deadBucket)
		meta := output.Bucket(metaBucket)
		activeBytes := readCounter(meta, activeBytesKey)
		deadBytes := readCounter(meta, deadBytesKey)

		for _, id := range ids {
			pendingKey := sequenceKey(id)
			value := pending.Get(pendingKey)
			if value == nil {
				continue
			}
			deadValue := encodeDeadRecord(reason, value)
			sequence, err := dead.NextSequence()
			if err != nil {
				return err
			}
			deadKey := sequenceKey(sequence)
			if err := dead.Put(deadKey, deadValue); err != nil {
				return err
			}
			if err := pending.Delete(pendingKey); err != nil {
				return err
			}
			activeBytes -= int64(len(pendingKey) + len(value))
			deadBytes += int64(len(deadKey) + len(deadValue))
		}

		// dead-letter 与活动队列分别有界，避免配置错误无限占满磁盘。
		cursor := dead.Cursor()
		for deadBytes > backend.maxBuffered {
			key, value := cursor.First()
			if key == nil {
				break
			}
			deadBytes -= int64(len(key) + len(value))
			if err := cursor.Delete(); err != nil {
				return err
			}
			evicted++
		}

		if activeBytes < 0 {
			activeBytes = 0
		}
		if deadBytes < 0 {
			deadBytes = 0
		}
		if err := writeCounter(meta, activeBytesKey, activeBytes); err != nil {
			return err
		}
		return writeCounter(meta, deadBytesKey, deadBytes)
	})
	return evicted, err
}

func (q *durableQueue) wake(name string) {
	select {
	case q.wakes[name] <- struct{}{}:
	default:
	}
}

func (q *durableQueue) waitRetry(interval time.Duration) bool {
	if interval <= 0 {
		interval = queueInitialRetry
	}
	// 抖动范围为 50%~100%，防止多个 output 同时冲击恢复中的后端。
	delay := interval/2 + time.Duration(rand.Int63n(int64(interval/2)+1))
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-q.stop:
		return false
	case <-timer.C:
		return true
	}
}

func (q *durableQueue) Close() error {
	var err error
	q.close.Do(func() {
		q.cancel()
		close(q.stop)
		q.wg.Wait()
		err = q.db.Close()
	})
	return err
}

func nextRetry(current, maximum time.Duration) time.Duration {
	if maximum <= 0 {
		maximum = DefaultMaxDelayInterval
	}
	if current <= 0 {
		return queueInitialRetry
	}
	next := current * 2
	if next > maximum {
		return maximum
	}
	return next
}

func isRetryableStatus(status int) bool {
	return status == 408 || status == 425 || status == 429 || status/100 == 5
}

func responseFailure(resp *responseData) string {
	body := strings.TrimSpace(string(resp.Body))
	if len(body) > 512 {
		body = body[:512]
	}
	if body == "" {
		return fmt.Sprintf("HTTP %d", resp.StatusCode)
	}
	return fmt.Sprintf("HTTP %d: %s", resp.StatusCode, body)
}

func encodeRecord(record durableRecord) ([]byte, error) {
	if uint64(len(record.query)) > uint64(^uint32(0)) ||
		uint64(len(record.auth)) > uint64(^uint32(0)) ||
		uint64(len(record.body)) > uint64(^uint32(0)) {
		return nil, errors.New("持久化记录过大")
	}

	result := make([]byte, 12+len(record.query)+len(record.auth)+len(record.body))
	binary.BigEndian.PutUint32(result[0:4], uint32(len(record.query)))
	binary.BigEndian.PutUint32(result[4:8], uint32(len(record.auth)))
	binary.BigEndian.PutUint32(result[8:12], uint32(len(record.body)))
	offset := 12
	copy(result[offset:], record.query)
	offset += len(record.query)
	copy(result[offset:], record.auth)
	offset += len(record.auth)
	copy(result[offset:], record.body)
	return result, nil
}

func decodeRecord(value []byte) (durableRecord, error) {
	if len(value) < 12 {
		return durableRecord{}, errors.New("记录头长度不足")
	}
	queryLength := uint64(binary.BigEndian.Uint32(value[0:4]))
	authLength := uint64(binary.BigEndian.Uint32(value[4:8]))
	bodyLength := uint64(binary.BigEndian.Uint32(value[8:12]))
	if 12+queryLength+authLength+bodyLength != uint64(len(value)) {
		return durableRecord{}, errors.New("记录长度不一致")
	}

	queryEnd := 12 + int(queryLength)
	authEnd := queryEnd + int(authLength)
	return durableRecord{
		query: string(value[12:queryEnd]),
		auth:  string(value[queryEnd:authEnd]),
		body:  append([]byte(nil), value[authEnd:]...),
	}, nil
}

func encodeDeadRecord(reason string, record []byte) []byte {
	result := make([]byte, 4+len(reason)+len(record))
	binary.BigEndian.PutUint32(result[:4], uint32(len(reason)))
	copy(result[4:], reason)
	copy(result[4+len(reason):], record)
	return result
}

func sequenceKey(sequence uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, sequence)
	return key
}

func readCounter(bucket *bolt.Bucket, key []byte) int64 {
	value := bucket.Get(key)
	if len(value) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(value))
}

func writeCounter(bucket *bolt.Bucket, key []byte, value int64) error {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, uint64(value))
	return bucket.Put(key, encoded)
}

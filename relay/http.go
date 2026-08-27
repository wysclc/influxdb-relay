package relay

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb1-client/models"
)

// HTTP is a relay for HTTP influxdb writes
type HTTP struct {
	addr   string
	name   string
	schema string

	cert string
	rp   string

	closing int64
	l       net.Listener
	server  *http.Server
	mu      sync.Mutex
	stop    sync.Once
	stopped chan struct{}

	backends []*httpBackend
	queue    *durableQueue
}

const (
	DefaultHTTPTimeout      = 10 * time.Second
	DefaultMaxDelayInterval = 10 * time.Second
	DefaultBatchSizeKB      = 512
	DefaultQueueDir         = "/var/lib/influxdb-relay"
	DefaultShutdownTimeout  = 30 * time.Second

	KB = 1024
	MB = 1024 * KB
)

func NewHTTP(cfg HTTPConfig) (Relay, error) {
	h := new(HTTP)
	h.stopped = make(chan struct{})

	h.addr = cfg.Addr
	h.name = cfg.Name

	h.cert = cfg.SSLCombinedPem
	h.rp = cfg.DefaultRetentionPolicy

	h.schema = "http"
	if h.cert != "" {
		h.schema = "https"
	}

	buffered := 0
	backendNames := make(map[string]struct{}, len(cfg.Outputs))
	for i := range cfg.Outputs {
		backend, err := newHTTPBackend(&cfg.Outputs[i])
		if err != nil {
			return nil, err
		}
		if _, exists := backendNames[backend.name]; exists {
			return nil, fmt.Errorf("HTTP relay %q 存在重复的 output 名称 %q", h.Name(), backend.name)
		}
		backendNames[backend.name] = struct{}{}
		if backend.maxBuffered > 0 {
			buffered++
		}

		h.backends = append(h.backends, backend)
	}
	if len(h.backends) == 0 {
		return nil, fmt.Errorf("HTTP relay %q 至少需要一个 output", h.Name())
	}

	// 为保持同一 relay 的 ACK 和投递语义一致，不允许混用同步和持久化模式。
	if buffered != 0 && buffered != len(h.backends) {
		return nil, fmt.Errorf("HTTP relay %q 必须为全部 output 配置 buffer-size-mb，或全部关闭", h.Name())
	}
	if buffered == len(h.backends) {
		queuePath := cfg.QueuePath
		if queuePath == "" {
			queuePath = defaultQueuePath(h.Name())
		}
		queue, err := newDurableQueue(queuePath, h.backends)
		if err != nil {
			return nil, err
		}
		h.queue = queue
		log.Printf("HTTP relay %q 使用持久化队列 %q", h.Name(), queuePath)
	}

	return h, nil
}

func (h *HTTP) Name() string {
	if h.name == "" {
		return fmt.Sprintf("%s://%s", h.schema, h.addr)
	}
	return h.name
}

func (h *HTTP) Run() error {
	l, err := net.Listen("tcp", h.addr)
	if err != nil {
		h.finish()
		return err
	}

	// support HTTPS
	if h.cert != "" {
		cert, err := tls.LoadX509KeyPair(h.cert, h.cert)
		if err != nil {
			_ = l.Close()
			h.finish()
			return err
		}

		l = tls.NewListener(l, &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
	}

	h.mu.Lock()
	h.l = l
	h.server = &http.Server{Handler: h}
	server := h.server
	h.mu.Unlock()

	log.Printf("Starting %s relay %q on %v", strings.ToUpper(h.schema), h.Name(), h.addr)

	err = server.Serve(l)
	if atomic.LoadInt64(&h.closing) != 0 {
		<-h.stopped
		return nil
	}
	h.finish()
	return err
}

func (h *HTTP) Stop() error {
	atomic.StoreInt64(&h.closing, 1)

	var stopErr error
	h.stop.Do(func() {
		h.mu.Lock()
		server := h.server
		listener := h.l
		h.mu.Unlock()

		if server != nil {
			ctx, cancel := context.WithTimeout(context.Background(), DefaultShutdownTimeout)
			stopErr = server.Shutdown(ctx)
			cancel()
			if stopErr != nil {
				_ = server.Close()
			}
		} else if listener != nil {
			stopErr = listener.Close()
		}

		if h.queue != nil {
			if err := h.queue.Close(); stopErr == nil {
				stopErr = err
			}
		}
		close(h.stopped)
	})
	return stopErr
}

func (h *HTTP) finish() {
	h.stop.Do(func() {
		if h.queue != nil {
			_ = h.queue.Close()
		}
		close(h.stopped)
	})
}

func (h *HTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	if r.URL.Path == "/ping" && (r.Method == "GET" || r.Method == "HEAD") {
		w.Header().Add("X-InfluxDB-Version", "relay")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if r.URL.Path != "/write" {
		jsonError(w, http.StatusNotFound, "invalid write endpoint")
		return
	}

	if r.Method != "POST" {
		w.Header().Set("Allow", "POST")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
		} else {
			jsonError(w, http.StatusMethodNotAllowed, "invalid write method")
		}
		return
	}

	queryParams := r.URL.Query()

	// fail early if we're missing the database
	if queryParams.Get("db") == "" {
		jsonError(w, http.StatusBadRequest, "missing parameter: db")
		return
	}

	if queryParams.Get("rp") == "" && h.rp != "" {
		queryParams.Set("rp", h.rp)
	}

	var body = r.Body

	if r.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(r.Body)
		if err != nil {
			jsonError(w, http.StatusBadRequest, "unable to decode gzip body")
		}
		defer b.Close()
		body = b
	}

	bodyBuf := getBuf()
	_, err := bodyBuf.ReadFrom(body)
	if err != nil {
		putBuf(bodyBuf)
		jsonError(w, http.StatusInternalServerError, "problem reading request body")
		return
	}

	precision := queryParams.Get("precision")
	points, err := models.ParsePointsWithPrecision(bodyBuf.Bytes(), start, precision)
	if err != nil {
		putBuf(bodyBuf)
		jsonError(w, http.StatusBadRequest, "unable to parse points")
		return
	}

	outBuf := getBuf()
	for _, p := range points {
		if _, err = outBuf.WriteString(p.PrecisionString(precision)); err != nil {
			break
		}
		if err = outBuf.WriteByte('\n'); err != nil {
			break
		}
	}

	// done with the input points
	putBuf(bodyBuf)

	if err != nil {
		putBuf(outBuf)
		jsonError(w, http.StatusInternalServerError, "problem writing points")
		return
	}

	// normalize query string
	query := queryParams.Encode()

	outBytes := outBuf.Bytes()

	// check for authorization performed via the header
	authHeader := r.Header.Get("Authorization")

	if h.queue != nil {
		_, err := h.queue.enqueue(outBytes, query, authHeader)
		putBuf(outBuf)
		if err != nil {
			log.Printf("HTTP relay %q 写入持久化队列失败: %v", h.Name(), err)
			jsonError(w, http.StatusServiceUnavailable, "无法持久化写入请求")
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(h.backends))

	var responses = make(chan *responseData, len(h.backends))

	for _, b := range h.backends {
		b := b
		go func() {
			defer wg.Done()
			resp, err := b.post(r.Context(), outBytes, query, authHeader)
			if err != nil {
				log.Printf("Problem posting to relay %q backend %q: %v", h.Name(), b.name, err)
			} else {
				if resp.StatusCode/100 == 5 {
					log.Printf("5xx response for relay %q backend %q: %v", h.Name(), b.name, resp.StatusCode)
				}
				responses <- resp
				log.Printf("%s write %.1fkb, statusCode %d, take %v\n",
					b.name,
					float64(len(outBytes))/1024,
					resp.StatusCode,
					time.Now().Sub(start),
				)
			}
		}()
	}

	go func() {
		wg.Wait()
		close(responses)
		putBuf(outBuf)
	}()

	var errResponse *responseData

	for resp := range responses {
		switch resp.StatusCode / 100 {
		case 2:
			w.WriteHeader(http.StatusNoContent)
			return

		case 4:
			// user error
			resp.Write(w)
			return

		default:
			// hold on to one of the responses to return back to the client
			errResponse = resp
		}
	}

	// no successful writes
	if errResponse == nil {
		// failed to make any valid request...
		jsonError(w, http.StatusServiceUnavailable, "unable to write points")
		return
	}

	errResponse.Write(w)
}

type responseData struct {
	backendName     string
	ContentType     string
	ContentEncoding string
	StatusCode      int
	Body            []byte
}

func (rd *responseData) Write(w http.ResponseWriter) {
	if rd.ContentType != "" {
		w.Header().Set("Content-Type", rd.ContentType)
	}

	if rd.ContentEncoding != "" {
		w.Header().Set("Content-Encoding", rd.ContentEncoding)
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(rd.Body)))
	w.WriteHeader(rd.StatusCode)
	w.Write(rd.Body)
}

func jsonError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	data := fmt.Sprintf("{\"error\":%q}\n", message)
	w.Header().Set("Content-Length", fmt.Sprint(len(data)))
	w.WriteHeader(code)
	w.Write([]byte(data))
}

type poster interface {
	post(context.Context, []byte, string, string) (*responseData, error)
}

type simplePoster struct {
	client   *http.Client
	location string
}

func newSimplePoster(location string, timeout time.Duration, skipTLSVerification bool) *simplePoster {
	// Configure custom transport for http.Client
	// Used for support skip-tls-verification option
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: skipTLSVerification,
		},
	}

	return &simplePoster{
		client: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		location: location,
	}
}

func (b *simplePoster) post(ctx context.Context, buf []byte, query string, auth string) (*responseData, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", b.location, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}

	req.URL.RawQuery = query
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}

	data, readErr := ioutil.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}

	return &responseData{
		ContentType:     resp.Header.Get("Content-Type"),
		ContentEncoding: resp.Header.Get("Content-Encoding"),
		StatusCode:      resp.StatusCode,
		Body:            data,
	}, nil
}

type httpBackend struct {
	poster
	name        string
	maxBuffered int64
	maxBatch    int
	maxDelay    time.Duration
}

func newHTTPBackend(cfg *HTTPOutputConfig) (*httpBackend, error) {
	if cfg.Name == "" {
		cfg.Name = cfg.Location
	}

	timeout := DefaultHTTPTimeout
	if cfg.Timeout != "" {
		t, err := time.ParseDuration(cfg.Timeout)
		if err != nil {
			return nil, fmt.Errorf("error parsing HTTP timeout '%v'", err)
		}
		timeout = t
	}

	if cfg.BufferSizeMB < 0 {
		return nil, errors.New("buffer-size-mb 不能为负数")
	}
	max := DefaultMaxDelayInterval
	if cfg.MaxDelayInterval != "" {
		m, err := time.ParseDuration(cfg.MaxDelayInterval)
		if err != nil {
			return nil, fmt.Errorf("error parsing max retry time %v", err)
		}
		if m <= 0 {
			return nil, errors.New("max-delay-interval 必须大于 0")
		}
		max = m
	}

	batch := DefaultBatchSizeKB * KB
	if cfg.MaxBatchKB > 0 {
		batch = cfg.MaxBatchKB * KB
	} else if cfg.MaxBatchKB < 0 {
		return nil, errors.New("max-batch-kb 不能为负数")
	}

	return &httpBackend{
		poster:      newSimplePoster(cfg.Location, timeout, cfg.SkipTLSVerification),
		name:        cfg.Name,
		maxBuffered: int64(cfg.BufferSizeMB) * MB,
		maxBatch:    batch,
		maxDelay:    max,
	}, nil
}

func defaultQueuePath(name string) string {
	safeName := strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, name)
	if safeName == "" {
		safeName = "http-relay"
	}
	sum := sha256.Sum256([]byte(name))
	return filepath.Join(DefaultQueueDir, fmt.Sprintf("%s-%x.queue.db", safeName, sum[:4]))
}

var bufPool = sync.Pool{New: func() interface{} { return new(bytes.Buffer) }}

func getBuf() *bytes.Buffer {
	if bb, ok := bufPool.Get().(*bytes.Buffer); ok {
		return bb
	}
	return new(bytes.Buffer)
}

func putBuf(b *bytes.Buffer) {
	b.Reset()
	bufPool.Put(b)
}

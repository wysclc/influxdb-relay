package relay

import (
	"bytes"
	"compress/gzip"
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSimplePosterSendsGZIPBody(t *testing.T) {
	original := bytes.Repeat([]byte("cpu,host=test value=1i 1\n"), 1000)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Content-Encoding"); got != "gzip" {
			t.Errorf("Content-Encoding 为 %q，期望 gzip", got)
		}
		reader, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Errorf("创建 gzip reader 失败: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		body, err := ioutil.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			t.Errorf("读取 gzip 正文失败: %v", err)
		}
		if !bytes.Equal(body, original) {
			t.Errorf("解压后的请求体与原文不同")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	poster := newSimplePoster(server.URL+"/write", time.Second, false, ipFamilyAuto, "gzip-test", httpCompressionGZIP, 1)
	response, err := poster.post(context.Background(), original, "db=test", "")
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusNoContent || !response.compressed {
		t.Fatalf("gzip 请求结果错误：%#v", response)
	}
	if response.requestBytes >= len(original) {
		t.Fatalf("gzip 未减少出站正文：original=%d wire=%d", len(original), response.requestBytes)
	}
}

func TestSimplePosterSkipsGZIPBelowThreshold(t *testing.T) {
	original := []byte("cpu value=1i 1\n")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if encoding := r.Header.Get("Content-Encoding"); encoding != "" {
			t.Errorf("小请求不应压缩，Content-Encoding=%q", encoding)
		}
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		if !bytes.Equal(body, original) {
			t.Errorf("未压缩请求体与原文不同")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	poster := newSimplePoster(server.URL+"/write", time.Second, false, ipFamilyAuto, "plain-test", httpCompressionGZIP, len(original)+1)
	response, err := poster.post(context.Background(), original, "db=test", "")
	if err != nil {
		t.Fatal(err)
	}
	if response.compressed || response.requestBytes != len(original) {
		t.Fatalf("小请求传输统计错误：%#v", response)
	}
}

func TestCompressHTTPBodyKeepsSmallerRepresentation(t *testing.T) {
	original := []byte{0x00, 0x01, 0x02}
	body, compressed, err := compressHTTPBody(original, httpCompressionGZIP, 0)
	if err != nil {
		t.Fatal(err)
	}
	if compressed || !bytes.Equal(body, original) {
		t.Fatalf("gzip 结果变大时应保留原文：compressed=%t body=%v", compressed, body)
	}
}

func TestHTTPCompressionConfiguration(t *testing.T) {
	backend, err := newHTTPBackend(&HTTPOutputConfig{
		Name:             "compressed",
		Location:         "http://127.0.0.1:8086/write",
		Compression:      "gzip",
		CompressionMinKB: 32,
	})
	if err != nil {
		t.Fatal(err)
	}
	if backend.compression != httpCompressionGZIP || backend.compressionMin != 32*KB {
		t.Fatalf("gzip 配置解析错误：%#v", backend)
	}

	_, err = newHTTPBackend(&HTTPOutputConfig{
		Location:    "http://127.0.0.1:8086/write",
		Compression: "brotli",
	})
	if err == nil || !strings.Contains(err.Error(), "compression") {
		t.Fatalf("应拒绝未知压缩算法，实际错误：%v", err)
	}

	_, err = newHTTPBackend(&HTTPOutputConfig{
		Location:         "http://127.0.0.1:8086/write",
		CompressionMinKB: -1,
	})
	if err == nil || !strings.Contains(err.Error(), "compression-min-kb") {
		t.Fatalf("应拒绝负数压缩阈值，实际错误：%v", err)
	}
}

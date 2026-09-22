package relay

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"strings"
)

type httpCompression string

const (
	httpCompressionNone httpCompression = "none"
	httpCompressionGZIP httpCompression = "gzip"

	DefaultCompressionMinKB = 64
)

func parseHTTPCompression(value string) (httpCompression, error) {
	compression := httpCompression(strings.ToLower(strings.TrimSpace(value)))
	if compression == "" {
		return httpCompressionNone, nil
	}
	switch compression {
	case httpCompressionNone, httpCompressionGZIP:
		return compression, nil
	default:
		return "", fmt.Errorf("compression %q 无效，可选值为 none、gzip", value)
	}
}

func compressHTTPBody(body []byte, compression httpCompression, minimum int) ([]byte, bool, error) {
	if compression != httpCompressionGZIP || len(body) < minimum {
		return body, false, nil
	}

	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(body); err != nil {
		_ = writer.Close()
		return nil, false, fmt.Errorf("gzip 压缩 HTTP 请求体失败: %v", err)
	}
	if err := writer.Close(); err != nil {
		return nil, false, fmt.Errorf("结束 gzip 压缩失败: %v", err)
	}
	if compressed.Len() >= len(body) {
		return body, false, nil
	}
	return compressed.Bytes(), true, nil
}

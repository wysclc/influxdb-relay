package relay

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPreferIPv6DialsIPv6First(t *testing.T) {
	var calls []string
	dialer := testIPFamilyDialer(ipFamilyPreferIPv6)
	dialer.lookup = func(context.Context, string) ([]net.IPAddr, error) {
		// 即使解析器先返回 IPv4，策略也必须先尝试 IPv6。
		return []net.IPAddr{
			{IP: net.ParseIP("192.0.2.10")},
			{IP: net.ParseIP("2001:db8::10")},
		}, nil
	}
	dialer.dial = func(_ context.Context, network, address string) (net.Conn, error) {
		calls = append(calls, network+" "+address)
		return newTestConn(), nil
	}

	conn, err := dialer.DialContext(context.Background(), "tcp", "dual.example:8086")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if len(calls) != 1 || calls[0] != "tcp6 [2001:db8::10]:8086" {
		t.Fatalf("IPv6 优先拨号顺序错误：%v", calls)
	}
}

func TestPreferIPv6FallsBackToIPv4(t *testing.T) {
	var mu sync.Mutex
	var calls []string
	dialer := testIPFamilyDialer(ipFamilyPreferIPv6)
	dialer.fallbackDelay = 5 * time.Millisecond
	dialer.lookup = func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{
			{IP: net.ParseIP("2001:db8::10")},
			{IP: net.ParseIP("192.0.2.10")},
		}, nil
	}
	dialer.dial = func(ctx context.Context, network, address string) (net.Conn, error) {
		mu.Lock()
		calls = append(calls, network+" "+address)
		mu.Unlock()
		if network == "tcp6" {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return newTestConn(), nil
	}

	conn, err := dialer.DialContext(context.Background(), "tcp", "dual.example:8086")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 2 || !strings.HasPrefix(calls[0], "tcp6 ") || !strings.HasPrefix(calls[1], "tcp4 ") {
		t.Fatalf("IPv4 回退顺序错误：%v", calls)
	}
}

func TestIPv6OnlyRejectsIPv4OnlyDomain(t *testing.T) {
	dialer := testIPFamilyDialer(ipFamilyIPv6Only)
	dialer.lookup = func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("192.0.2.10")}}, nil
	}
	_, err := dialer.DialContext(context.Background(), "tcp", "ipv4.example:8086")
	if err == nil || !strings.Contains(err.Error(), "没有可用的 IPv6") {
		t.Fatalf("ipv6-only 应拒绝仅有 IPv4 的域名，实际错误：%v", err)
	}
}

func TestIPv4OnlyUsesIPv4(t *testing.T) {
	dialer := testIPFamilyDialer(ipFamilyIPv4Only)
	dialer.lookup = func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{
			{IP: net.ParseIP("2001:db8::10")},
			{IP: net.ParseIP("192.0.2.10")},
		}, nil
	}
	dialer.dial = func(_ context.Context, network, address string) (net.Conn, error) {
		if network != "tcp4" || address != "192.0.2.10:8086" {
			t.Fatalf("ipv4-only 使用了错误地址：%s %s", network, address)
		}
		return newTestConn(), nil
	}
	conn, err := dialer.DialContext(context.Background(), "tcp", "dual.example:8086")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
}

func TestIPFamilyConfiguration(t *testing.T) {
	backend, err := newHTTPBackend(&HTTPOutputConfig{
		Name:       "dual-stack",
		Location:   "http://dual.example:8086/write",
		IPFamily:   "prefer-ipv6",
		MaxBatchKB: 512,
	})
	if err != nil {
		t.Fatal(err)
	}
	if backend.ipFamily != ipFamilyPreferIPv6 {
		t.Fatalf("地址族策略解析错误：%q", backend.ipFamily)
	}

	_, err = newHTTPBackend(&HTTPOutputConfig{
		Location: "http://dual.example:8086/write",
		IPFamily: "ipv7",
	})
	if err == nil || !strings.Contains(err.Error(), "ip-family") {
		t.Fatalf("应拒绝无效地址族策略，实际错误：%v", err)
	}
}

func testIPFamilyDialer(family ipFamily) *ipFamilyDialer {
	return &ipFamilyDialer{
		family:        family,
		fallbackDelay: time.Second,
	}
}

func newTestConn() net.Conn {
	client, server := net.Pipe()
	_ = server.Close()
	return client
}

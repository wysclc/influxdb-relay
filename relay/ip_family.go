package relay

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"
)

type ipFamily string

const (
	ipFamilyAuto       ipFamily = "auto"
	ipFamilyPreferIPv6 ipFamily = "prefer-ipv6"
	ipFamilyIPv6Only   ipFamily = "ipv6-only"
	ipFamilyIPv4Only   ipFamily = "ipv4-only"

	ipv6FallbackDelay  = 300 * time.Millisecond
	defaultDialTimeout = 30 * time.Second
	defaultKeepAlive   = 30 * time.Second
)

type ipLookupFunc func(context.Context, string) ([]net.IPAddr, error)
type networkDialFunc func(context.Context, string, string) (net.Conn, error)

type ipFamilyDialer struct {
	family        ipFamily
	lookup        ipLookupFunc
	dial          networkDialFunc
	fallbackDelay time.Duration
}

type familyDialResult struct {
	conn    net.Conn
	err     error
	network string
}

func parseIPFamily(value string) (ipFamily, error) {
	family := ipFamily(strings.ToLower(strings.TrimSpace(value)))
	if family == "" {
		return ipFamilyAuto, nil
	}
	switch family {
	case ipFamilyAuto, ipFamilyPreferIPv6, ipFamilyIPv6Only, ipFamilyIPv4Only:
		return family, nil
	default:
		return "", fmt.Errorf("ip-family %q 无效，可选值为 auto、prefer-ipv6、ipv6-only、ipv4-only", value)
	}
}

func newIPFamilyDialer(family ipFamily) *ipFamilyDialer {
	dialer := &net.Dialer{
		Timeout:   defaultDialTimeout,
		KeepAlive: defaultKeepAlive,
	}
	return &ipFamilyDialer{
		family:        family,
		lookup:        net.DefaultResolver.LookupIPAddr,
		dial:          dialer.DialContext,
		fallbackDelay: ipv6FallbackDelay,
	}
}

func (d *ipFamilyDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	addresses, err := d.resolve(ctx, host)
	if err != nil {
		return nil, err
	}
	var ipv6, ipv4 []net.IPAddr
	for _, address := range addresses {
		if address.IP.To4() == nil {
			ipv6 = append(ipv6, address)
		} else {
			ipv4 = append(ipv4, address)
		}
	}

	switch d.family {
	case ipFamilyIPv6Only:
		if len(ipv6) == 0 {
			return nil, fmt.Errorf("域名 %q 没有可用的 IPv6 地址", host)
		}
		return d.dialAddresses(ctx, "tcp6", ipv6, port)
	case ipFamilyIPv4Only:
		if len(ipv4) == 0 {
			return nil, fmt.Errorf("域名 %q 没有可用的 IPv4 地址", host)
		}
		return d.dialAddresses(ctx, "tcp4", ipv4, port)
	case ipFamilyPreferIPv6:
		return d.dialPreferIPv6(ctx, address, ipv6, ipv4, port)
	default:
		return nil, fmt.Errorf("不支持的地址族策略 %q", d.family)
	}
}

func (d *ipFamilyDialer) resolve(ctx context.Context, host string) ([]net.IPAddr, error) {
	ipHost, zone := host, ""
	if index := strings.LastIndex(host, "%"); index >= 0 {
		ipHost, zone = host[:index], host[index+1:]
	}
	if ip := net.ParseIP(ipHost); ip != nil {
		return []net.IPAddr{{IP: ip, Zone: zone}}, nil
	}
	addresses, err := d.lookup(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("解析域名 %q 失败: %v", host, err)
	}
	if len(addresses) == 0 {
		return nil, fmt.Errorf("域名 %q 没有可用的 IP 地址", host)
	}
	return addresses, nil
}

func (d *ipFamilyDialer) dialAddresses(ctx context.Context, network string, addresses []net.IPAddr, port string) (net.Conn, error) {
	var lastErr error
	for _, address := range addresses {
		host := address.IP.String()
		if address.Zone != "" {
			host += "%" + address.Zone
		}
		conn, err := d.dial(ctx, network, net.JoinHostPort(host, port))
		if err == nil {
			return conn, nil
		}
		lastErr = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, lastErr
}

func (d *ipFamilyDialer) dialPreferIPv6(ctx context.Context, original string, ipv6, ipv4 []net.IPAddr, port string) (net.Conn, error) {
	if len(ipv6) == 0 {
		if len(ipv4) == 0 {
			return nil, fmt.Errorf("地址 %q 没有可用的 IP 地址", original)
		}
		return d.dialAddresses(ctx, "tcp4", ipv4, port)
	}
	if len(ipv4) == 0 {
		return d.dialAddresses(ctx, "tcp6", ipv6, port)
	}

	raceCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan familyDialResult)
	start := func(network string, addresses []net.IPAddr) {
		go func() {
			conn, err := d.dialAddresses(raceCtx, network, addresses, port)
			result := familyDialResult{conn: conn, err: err, network: network}
			select {
			case results <- result:
			case <-raceCtx.Done():
				if conn != nil {
					_ = conn.Close()
				}
			}
		}()
	}

	start("tcp6", ipv6)
	timer := time.NewTimer(d.fallbackDelay)
	defer timer.Stop()
	ipv4Started := false
	active := 1
	var ipv6Err, ipv4Err error
	for active > 0 {
		select {
		case result := <-results:
			active--
			if result.err == nil {
				return result.conn, nil
			}
			if result.network == "tcp6" {
				ipv6Err = result.err
				if !ipv4Started {
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					start("tcp4", ipv4)
					ipv4Started = true
					active++
				}
			} else {
				ipv4Err = result.err
			}
		case <-timer.C:
			if !ipv4Started {
				start("tcp4", ipv4)
				ipv4Started = true
				active++
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("连接 %q 失败（IPv6: %v；IPv4: %v）", original, ipv6Err, ipv4Err)
}

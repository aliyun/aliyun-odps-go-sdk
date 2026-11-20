package restclient

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	testDomain    = "localhost" // 使用本地域名保证可靠性
	testPort      = "0"         // 系统分配端口
	testCacheTime = 2           // 缓存时间（秒）
)

// 真实DNS解析测试
func TestE2E(t *testing.T) {
	// 创建测试服务器
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	testPort := listener.Addr().(*net.TCPAddr).Port
	testAddress := net.JoinHostPort(testDomain, strconv.Itoa(testPort))

	// 创建测试组件
	resolver := NewResolver(testCacheTime)
	dialer := &Dialer{
		Resolver: resolver,
		Dialer:   net.Dialer{Timeout: time.Second},
	}

	t.Run("DNSCache", func(t *testing.T) {
		// 首次查询（真实DNS）
		start := time.Now()
		ips, err := resolver.Lookup(context.Background(), testDomain)
		if err != nil {
			t.Fatal(err)
		}
		if len(ips) == 0 {
			t.Fatal("No IPs found")
		}
		t.Logf("Initial lookup took %v", time.Since(start))

		// 验证缓存命中
		start = time.Now()
		ips2, err := resolver.Lookup(context.Background(), testDomain)
		if err != nil {
			t.Fatal(err)
		}
		if !equalSlices(ips, ips2) {
			t.Error("Cache mismatch")
		}
		t.Logf("Cached lookup took %v", time.Since(start))

		// 等待缓存过期
		time.Sleep(time.Duration(testCacheTime+1) * time.Second)

		// 验证缓存过期
		start = time.Now()
		_, err = resolver.Lookup(context.Background(), testDomain)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("Expired lookup took %v", time.Since(start))
	})

	t.Run("DialConnection", func(t *testing.T) {
		conn, err := dialer.DialContext(context.Background(), "tcp", testAddress)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		// 验证连接地址
		remoteAddr := conn.RemoteAddr().String()
		expected := listener.Addr().String()
		if remoteAddr != expected {
			t.Errorf("Expected connection to %s, got %s", expected, remoteAddr)
		}
	})
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestResolverLookupResultsAreIndependent(t *testing.T) {
	r := NewResolver(60)
	first, err := r.Lookup(t.Context(), "localhost")
	require.NoError(t, err)
	require.NotEmpty(t, first)
	address := first[0]
	first[0] = "192.0.2.1"

	cached, err := r.Lookup(t.Context(), "localhost")
	require.NoError(t, err)
	require.Equal(t, address, cached[0])
	cached[0] = "192.0.2.2"

	again, err := r.Lookup(t.Context(), "localhost")
	require.NoError(t, err)
	require.Equal(t, address, again[0])
}

func TestConcurrentDialsShareDNSCache(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	const host = "concurrent.invalid"
	r := NewResolver(60)
	r.cache[host] = cacheEntry{ips: []string{"127.0.0.1", "127.0.0.2"}, createTime: time.Now().Unix()}
	dialer := Dialer{Resolver: r, Dialer: net.Dialer{Resolver: noDNSResolver(), Timeout: time.Second}}
	address := net.JoinHostPort(host, strconv.Itoa(listener.Addr().(*net.TCPAddr).Port))

	var group sync.WaitGroup
	for range 32 {
		group.Go(func() {
			conn, err := dialer.DialContext(t.Context(), "tcp4", address)
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close()
		})
	}
	group.Wait()
	ips, err := r.Lookup(t.Context(), host)
	require.NoError(t, err)
	require.Equal(t, []string{"127.0.0.1", "127.0.0.2"}, ips)
}

func TestDialerUsesCachedIPv6Address(t *testing.T) {
	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6 loopback unavailable: %v", err)
	}
	defer listener.Close()
	const host = "ipv6.invalid"
	r := NewResolver(60)
	r.cache[host] = cacheEntry{ips: []string{"::1"}, createTime: time.Now().Unix()}
	dialer := Dialer{Resolver: r, Dialer: net.Dialer{Resolver: noDNSResolver(), Timeout: time.Second}}
	conn, err := dialer.DialContext(t.Context(), "tcp6", net.JoinHostPort(host, strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)))
	require.NoError(t, err)
	defer conn.Close()
	require.Equal(t, listener.Addr().String(), conn.RemoteAddr().String())
}

func noDNSResolver() *net.Resolver {
	return &net.Resolver{PreferGo: true, Dial: func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("fixture requires a cached address")
	}}
}

func TestDNSDialerValidatesAddressAndFallsBack(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	address := net.JoinHostPort("localhost", strconv.Itoa(listener.Addr().(*net.TCPAddr).Port))

	resolver := NewResolver(60)
	resolver.cache["localhost"] = cacheEntry{ips: []string{"127.0.0.2"}, createTime: time.Now().Unix()}
	dialer := Dialer{Resolver: resolver, Dialer: net.Dialer{Timeout: time.Second}}
	_, err = dialer.DialContext(t.Context(), "tcp4", "localhost")
	require.ErrorContains(t, err, "missing port")

	conn, err := dialer.DialContext(t.Context(), "tcp4", address)
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	plain := Dialer{Dialer: net.Dialer{Timeout: time.Second}}
	conn, err = plain.DialContext(t.Context(), "tcp4", address)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
}

func TestZeroResolverAndCancellation(t *testing.T) {
	var resolver Resolver
	addresses, err := resolver.Lookup(t.Context(), "localhost")
	require.NoError(t, err)
	require.NotEmpty(t, addresses)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = resolver.Lookup(ctx, "not-cached.invalid")
	require.Error(t, err)
}

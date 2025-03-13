package restclient

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"
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

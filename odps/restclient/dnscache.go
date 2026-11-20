package restclient

import (
	"context"
	"math/rand/v2"
	"net"
	"slices"
	"sync"
	"time"
)

const DefaultDNSCacheExpireTime = int64(600)

type Dialer struct {
	Resolver *Resolver
	Dialer   net.Dialer
}

func (dialer *Dialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if dialer.Resolver == nil {
		return dialer.Dialer.DialContext(ctx, network, address)
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}

	ips, _ := dialer.Resolver.Lookup(ctx, host)

	rand.Shuffle(len(ips), func(i, j int) {
		ips[i], ips[j] = ips[j], ips[i]
	})

	for _, ip := range ips {
		conn, err := dialer.Dialer.DialContext(ctx, network, net.JoinHostPort(ip, port))
		if err == nil {
			return conn, nil
		}
	}

	return dialer.Dialer.DialContext(ctx, network, address)
}

type Resolver struct {
	lock             sync.RWMutex
	cache            map[string]cacheEntry
	CacheTimeSeconds int64
}

type cacheEntry struct {
	ips        []string
	createTime int64
}

func NewResolver(cacheTime int64) *Resolver {
	return &Resolver{
		cache:            make(map[string]cacheEntry, 1),
		CacheTimeSeconds: cacheTime,
	}
}

func (r *Resolver) Lookup(ctx context.Context, host string) ([]string, error) {
	r.lock.RLock()
	ce, exists := r.cache[host]
	r.lock.RUnlock()

	now := time.Now().Unix()

	if exists {
		if now-ce.createTime >= r.CacheTimeSeconds {
			return r.lookup(ctx, host)
		}

		return slices.Clone(ce.ips), nil
	}

	return r.lookup(ctx, host)
}

func (r *Resolver) lookup(ctx context.Context, host string) ([]string, error) {
	// 调用默认的resolver
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}

	if len(ips) == 0 {
		return nil, nil
	}

	strIPs := make([]string, len(ips))
	for index, ip := range ips {
		strIPs[index] = ip.String()
	}

	r.lock.Lock()
	if r.cache == nil {
		r.cache = make(map[string]cacheEntry)
	}
	r.cache[host] = cacheEntry{ips: slices.Clone(strIPs), createTime: time.Now().Unix()}
	r.lock.Unlock()

	return strIPs, nil
}

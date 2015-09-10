// ketamaselector implements a list of servers where servers are picked based
// on Ketama consistent hashing algorithm
package ketamaselector

import (
	"net"
	"strings"
	"sync"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/dgryski/go-ketama"
)

type ServerSet struct {
	mu     sync.RWMutex
	addrs  map[string]net.Addr
	ketama *ketama.Continuum
}

func makeHostPort(s string) string {
	if !strings.Contains(s, ":") {
		return s + ":11211"
	}
	return s
}

// Sets buckets to be used. Label must be either a valid TCP address or
// a path to unix domain socket, in which case must contain a "/"
func (ss *ServerSet) SetBuckets(buckets ...ketama.Bucket) error {
	addrs := make(map[string]net.Addr)
	for _, b := range buckets {
		if strings.Contains(b.Label, "/") {
			addr, err := net.ResolveUnixAddr("unix", b.Label)
			if err != nil {
				return err
			}
			addrs[b.Label] = addr
		} else {
			tcpaddr, err := net.ResolveTCPAddr("tcp", b.Label)
			if err != nil {
				return err
			}
			addrs[makeHostPort(b.Label)] = tcpaddr
		}
	}

	ss.mu.Lock()
	defer ss.mu.Unlock()
	c, err := ketama.NewWithHash(buckets, ketama.HashFunc2)
	if err != nil {
		return err
	}
	ss.ketama = c
	ss.addrs = addrs

	return nil
}

// Sets servers. With this method, the weight for all the servers are
// all equal. You must use SetBuckets() if you want to control that.
func (ss *ServerSet) SetServers(servers ...string) error {
	buckets := make([]ketama.Bucket, len(servers))
	for i, server := range servers {
		buckets[i] = ketama.Bucket{
			Label:  server,
			Weight: 1,
		}
	}
	return ss.SetBuckets(buckets...)
}

func (ss *ServerSet) PickServer(key string) (net.Addr, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	if l := ss.ketama.Hash(key); l != "" {
		if addr, ok := ss.addrs[l]; ok {
			return addr, nil
		}
	}
	return nil, memcache.ErrNoServers
}

func (ss *ServerSet) Each(f func(net.Addr) error) error {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	for _, a := range ss.addrs {
		if err := f(a); nil != err {
			return err
		}
	}
	return nil
}
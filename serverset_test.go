package ketamaselector

import (
	"fmt"
	"net"
	"testing"
)

func TestFoo(t *testing.T) {
	ss := &ServerSet{}

	servers := make([]string, 10)
	for i := 1; i < 10; i++ {
		servers[i] = fmt.Sprintf("10.0.0.%d:11211", i)
	}

	var prev net.Addr
	for i := 0; i < 3; i++ {
		err := ss.SetServers(servers[i:]...)
		if err != nil {
			t.Errorf("Failed to SetServerS(): %s", err)
			return
		}

		addr, err := ss.PickServer("foo")
		if err != nil {
			t.Errorf("Failed to PickServer(): %s", err)
			return
		}
		t.Logf("Picked %s", addr.String())

		prev = addr
		if i == 0 {
			continue
		}

		if prev.String() != addr.String() {
			t.Errorf("Picked a different server!")
		}
	}
}
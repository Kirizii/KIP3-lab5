package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelectServer_ConsistentHashing(t *testing.T) {
	healthyServers = []string{"server1:8080", "server2:8080", "server3:8080"}

	addr := "192.168.0.1:54321"
	server1, err1 := selectServer(addr)
	server2, err2 := selectServer(addr)

	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.Equal(t, server1, server2, "selectServer must return same server for same address")
	assert.Contains(t, healthyServers, server1)
}

func TestSelectServer_EmptyHealthyList(t *testing.T) {
	healthyServers = []string{}
	_, err := selectServer("127.0.0.1:12345")
	assert.Error(t, err, "selectServer should fail with no healthy servers")
}

func TestSelectServer_Distribution(t *testing.T) {
	healthyServers = []string{"a:1", "b:2", "c:3"}

	counter := map[string]int{}
	for i := 0; i < 1000; i++ {
		addr := fakeAddr(i)
		srv, _ := selectServer(addr)
		counter[srv]++
	}

	for _, s := range healthyServers {
		assert.Greater(t, counter[s], 0, "each server should be used")
	}
}

func fakeAddr(i int) string {
	return "10.0.0." + string(rune(i%256)) + ":12345"
}

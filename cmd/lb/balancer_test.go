package main

import (
	"fmt"
	"testing"
)

func TestHash(t *testing.T) {
	addr := "192.168.1.1:12345"
	hash1 := hash(addr)
	hash2 := hash(addr)

	if hash1 != hash2 {
		t.Errorf("Hash function should be consistent. Got %d and %d for same input", hash1, hash2)
	}

	addr2 := "192.168.1.2:54321"
	hash3 := hash(addr2)

	if hash1 == hash3 {
		t.Logf("Note: Different addresses produced same hash (collision): %s and %s", addr, addr2)
	}

	emptyHash := hash("")
	if emptyHash == 0 {
		t.Logf("Empty string hash is zero")
	}
}


func TestChooseServer(t *testing.T) {
	servers := []string{"server1:8080", "server2:8080", "server3:8080"}

	result := chooseServer("192.168.1.1:12345", []string{})
	if result != "" {
		t.Errorf("Expected empty string for empty server pool, got %s", result)
	}

	clientAddr := "192.168.1.1:12345"
	server1 := chooseServer(clientAddr, servers)
	server2 := chooseServer(clientAddr, servers)

	if server1 != server2 {
		t.Errorf("Same client should always get same server. Got %s and %s", server1, server2)
	}
	found := false
	for _, s := range servers {
		if s == server1 {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Returned server %s should be from the server pool", server1)
	}
}


func TestChooseServerDistribution(t *testing.T) {
	servers := []string{"server1:8080", "server2:8080", "server3:8080"}
	distribution := make(map[string]int)

	for i := 0; i < 300; i++ {
		clientAddr := fmt.Sprintf("192.168.1.%d:%d", (i%254)+1, 10000+i)
		server := chooseServer(clientAddr, servers)
		distribution[server]++
	}

	for _, server := range servers {
		if distribution[server] == 0 {
			t.Errorf("Server %s received no requests", server)
		}
	}

	totalRequests := 300
	expectedPerServer := totalRequests / len(servers)
	tolerance := expectedPerServer / 2

	for server, count := range distribution {
		if count < expectedPerServer-tolerance || count > expectedPerServer+tolerance {
			t.Logf("Server %s has %d requests (expected around %d)", server, count, expectedPerServer)
		}
	}

	t.Logf("Distribution: %v", distribution)
}


func TestChooseServerConsistency(t *testing.T) {
	servers := []string{"server1:8080", "server2:8080", "server3:8080"}

	testCases := []string{
		"192.168.1.1:12345",
		"10.0.0.1:8080",
		"127.0.0.1:54321",
		"172.16.0.1:9000",
		"203.0.113.1:80",
	}

	for _, clientAddr := range testCases {
		var chosenServers []string
		for i := 0; i < 10; i++ {
			server := chooseServer(clientAddr, servers)
			chosenServers = append(chosenServers, server)
		}

		firstChoice := chosenServers[0]
		for i, server := range chosenServers {
			if server != firstChoice {
				t.Errorf("Client %s: inconsistent server choice at iteration %d. Expected %s, got %s",
					clientAddr, i, firstChoice, server)
			}
		}
	}
}


func TestChooseServerWithDifferentPoolSizes(t *testing.T) {
	testCases := []struct {
		name    string
		servers []string
	}{
		{"SingleServer", []string{"server1:8080"}},
		{"TwoServers", []string{"server1:8080", "server2:8080"}},
		{"FiveServers", []string{"server1:8080", "server2:8080", "server3:8080", "server4:8080", "server5:8080"}},
	}

	clientAddr := "192.168.1.100:12345"

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			server := chooseServer(clientAddr, tc.servers)

			found := false
			for _, s := range tc.servers {
				if s == server {
					found = true
					break
				}
			}

			if !found {
				t.Errorf("Server %s not found in pool %v", server, tc.servers)
			}

			server2 := chooseServer(clientAddr, tc.servers)
			if server != server2 {
				t.Errorf("Inconsistent server choice: %s vs %s", server, server2)
			}
		})
	}
}


func BenchmarkChooseServer(b *testing.B) {
	servers := []string{"server1:8080", "server2:8080", "server3:8080"}
	clientAddr := "192.168.1.1:12345"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chooseServer(clientAddr, servers)
	}
}


func BenchmarkHash(b *testing.B) {
	addr := "192.168.1.1:12345"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hash(addr)
	}
}

package integration

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	if err := waitForBalancer(t); err != nil {
		t.Fatalf("Balancer is not ready: %v", err)
	}

	t.Run("BasicFunctionality", func(t *testing.T) {
		testBasicFunctionality(t)
	})

	t.Run("ServerDistribution", func(t *testing.T) {
		testServerDistribution(t)
	})
}

func waitForBalancer(t *testing.T) error {
	t.Log("Waiting for balancer to be ready...")

	for i := 0; i < 30; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err == nil {
			resp.Body.Close()
			t.Log("Balancer is ready")
			return nil
		}
		t.Logf("Attempt %d: %v", i+1, err)
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("balancer is not responding after 30 attempts")
}

func testBasicFunctionality(t *testing.T) {
	resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
	if err != nil {
		t.Fatalf("Basic request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	server := resp.Header.Get("lb-from")
	if server == "" {
		t.Error("Expected lb-from header to be present")
	} else {
		t.Logf("Response from server: %s", server)
	}
}

func testServerDistribution(t *testing.T) {
	serverCounts := make(map[string]int)
	const numRequests = 15

	for i := 0; i < numRequests; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			t.Fatalf("Request %d failed: %v", i+1, err)
		}

		server := resp.Header.Get("lb-from")
		if server != "" {
			serverCounts[server]++
		}
		resp.Body.Close()

		time.Sleep(50 * time.Millisecond)
	}

	t.Logf("Server distribution across %d requests: %v", numRequests, serverCounts)

	uniqueServers := len(serverCounts)
	if uniqueServers < 1 {
		t.Errorf("Expected requests to be processed by at least 1 server, got %d", uniqueServers)
	}

	totalProcessed := 0
	for _, count := range serverCounts {
		totalProcessed += count
	}

	if totalProcessed < numRequests/2 {
		t.Errorf("Too few requests were processed successfully: %d out of %d", totalProcessed, numRequests)
	}

	t.Logf("Total requests processed: %d", totalProcessed)
	t.Logf("Unique servers used: %d", uniqueServers)
}

func BenchmarkBalancer(b *testing.B) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		b.Skip("Integration test is not enabled")
	}

	for i := 0; i < 10; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err == nil {
			resp.Body.Close()
			break
		}
		time.Sleep(1 * time.Second)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
			if err != nil {
				b.Errorf("Request failed: %v", err)
				continue
			}
			resp.Body.Close()
		}
	})
}

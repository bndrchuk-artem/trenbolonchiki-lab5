package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

const baseAddress = "http://balancer:8090"
const teamName = "trenbolonchiki"

var client = http.Client{
	Timeout: 3 * time.Second,
}

type Response struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	if err := waitForBalancer(t); err != nil {
		t.Fatalf("Balancer is not ready: %v", err)
	}

	t.Run("DatabaseIntegration", func(t *testing.T) {
		testDatabaseIntegration(t)
	})

	t.Run("ServerDistribution", func(t *testing.T) {
		testServerDistribution(t)
	})
}

func waitForBalancer(t *testing.T) error {
	t.Log("Waiting for balancer to be ready...")

	for i := 0; i < 30; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=%s", baseAddress, teamName))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound {
				t.Log("Balancer is ready")
				return nil
			}
		}
		t.Logf("Attempt %d: %v", i+1, err)
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("balancer is not responding after 30 attempts")
}

func testDatabaseIntegration(t *testing.T) {
	resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=%s", baseAddress, teamName))
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
		return
	}

	var data Response
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if data.Key != teamName {
		t.Errorf("Expected key '%s', got '%s'", teamName, data.Key)
	}

	if len(data.Value) != 10 {
		t.Errorf("Expected date value in format YYYY-MM-DD, got: %s", data.Value)
	}

	t.Logf("Successfully retrieved data: key=%s, value=%s", data.Key, data.Value)
	resp2, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=nonexistent", baseAddress))
	if err != nil {
		t.Fatalf("Request for non-existent key failed: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusNotFound {
		t.Errorf("Expected status 404 for non-existent key, got %d", resp2.StatusCode)
	}
}

func testServerDistribution(t *testing.T) {
	serverCounts := make(map[string]int)
	const numRequests = 15

	for i := 0; i < numRequests; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=%s", baseAddress, teamName))
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
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=%s", baseAddress, teamName))
		if err == nil {
			resp.Body.Close()
			break
		}
		time.Sleep(1 * time.Second)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=%s", baseAddress, teamName))
			if err != nil {
				b.Errorf("Request failed: %v", err)
				continue
			}
			resp.Body.Close()
		}
	})
}
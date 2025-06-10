package main

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/bndrchuk-artem/trenbolonchiki-lab5/httptools"
	"github.com/bndrchuk-artem/trenbolonchiki-lab5/signal"
)

var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var (
	timeout     = time.Duration(*timeoutSec) * time.Second
	serversPool = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}
	healthyServersMutex sync.RWMutex
	healthyServers      []string
)

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func chooseServer(clientAddr string, servers []string) string {
	if len(servers) == 0 {
		return ""
	}

	serverIndex := int(hash(clientAddr)) % len(servers)
	return servers[serverIndex]
}

func getHealthyServers() []string {
	healthyServersMutex.RLock()
	defer healthyServersMutex.RUnlock()

	result := make([]string, len(healthyServers))
	copy(result, healthyServers)
	return result
}

func updateHealthyServers() {
	var healthy []string

	for _, server := range serversPool {
		if health(server) {
			healthy = append(healthy, server)
		}
	}

	healthyServersMutex.Lock()
	healthyServers = healthy
	healthyServersMutex.Unlock()
}

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, _ := context.WithTimeout(r.Context(), timeout)
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", dst)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func main() {
	flag.Parse()

	updateHealthyServers()

	for _, server := range serversPool {
		server := server
		go func() {
			for range time.Tick(10 * time.Second) {
				isHealthy := health(server)
				log.Println(server, "healthy:", isHealthy)

				updateHealthyServers()
			}
		}()
	}

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		currentHealthyServers := getHealthyServers()

		if len(currentHealthyServers) == 0 {
			log.Println("No healthy servers available")
			rw.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		targetServer := chooseServer(r.RemoteAddr, currentHealthyServers)

		if targetServer == "" {
			log.Println("Failed to choose target server")
			rw.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		log.Printf("Forwarding request from %s to %s", r.RemoteAddr, targetServer)
		forward(targetServer, rw, r)
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}

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

	"github.com/roman-mazur/architecture-practice-4-template/httptools"
	"github.com/roman-mazur/architecture-practice-4-template/signal"
)

var (
	port         = flag.Int("port", 8090, "load balancer port")
	timeoutSec   = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https        = flag.Bool("https", false, "whether backends support HTTPs")
	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

var (
	timeout     = time.Duration(*timeoutSec) * time.Second
	serversPool = []string{
		"server1:8080",
		"server2:8080",
		"server3:8080",
	}

	healthyServers []string
	healthyMu      sync.RWMutex
)

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, _ := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

func updateHealthLoop() {
	for _, server := range serversPool {
		s := server
		go func() {
			for range time.Tick(5 * time.Second) {
				isHealthy := health(s)
				log.Println(s, "healthy:", isHealthy)

				healthyMu.Lock()
				if isHealthy {
					if !contains(healthyServers, s) {
						healthyServers = append(healthyServers, s)
					}
				} else {
					healthyServers = remove(healthyServers, s)
				}
				healthyMu.Unlock()
			}
		}()
	}
}

func contains(list []string, val string) bool {
	for _, item := range list {
		if item == val {
			return true
		}
	}
	return false
}

func remove(list []string, val string) []string {
	result := make([]string, 0, len(list))
	for _, item := range list {
		if item != val {
			result = append(result, item)
		}
	}
	return result
}

func selectServer(remoteAddr string) (string, error) {
	healthyMu.RLock()
	defer healthyMu.RUnlock()

	if len(healthyServers) == 0 {
		return "", fmt.Errorf("no healthy servers available")
	}

	hasher := fnv.New32a()
	hasher.Write([]byte(remoteAddr))
	index := int(hasher.Sum32()) % len(healthyServers)
	return healthyServers[index], nil
}

func forward(dst string, rw http.ResponseWriter, r *http.Request) error {
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err != nil {
		log.Printf("Failed to get response from %s: %s", dst, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
	defer resp.Body.Close()

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
	_, err = io.Copy(rw, resp.Body)
	if err != nil {
		log.Printf("Failed to write response: %s", err)
	}
	return nil
}

func main() {
	flag.Parse()
	timeout = time.Duration(*timeoutSec) * time.Second

	updateHealthLoop()

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		remoteAddr := r.Header.Get("X-Test-Client")
		if remoteAddr == "" {
			remoteAddr = r.RemoteAddr
		}

		dst, err := selectServer(remoteAddr)
		if err != nil {
			http.Error(rw, "No healthy servers available", http.StatusServiceUnavailable)
			return
		}
		forward(dst, rw, r)
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}

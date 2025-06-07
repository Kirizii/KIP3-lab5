package integration

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	serverSet := make(map[string]bool)

	for i := 0; i < 10; i++ {
		req, _ := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/some-data", baseAddress), nil)
		req.Header.Set("X-Test-Client", "client-123")

		resp, err := client.Do(req)
		if err != nil {
			t.Errorf("request failed: %s", err)
			continue
		}
		from := resp.Header.Get("lb-from")
		t.Logf("response from [%s]", from)
		serverSet[from] = true
		_ = resp.Body.Close()
	}

	assert.Equal(t, 1, len(serverSet), "Expected all responses from the same server")
}

func BenchmarkBalancer(b *testing.B) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		b.Skip("Integration benchmark is not enabled")
	}

	for i := 0; i < b.N; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=invlabs", baseAddress))
		if err != nil {
			b.Errorf("request failed: %s", err)
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
	}
}

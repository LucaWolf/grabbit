package grabbit

import (
	"context"
	"log"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"flag"

	"go.uber.org/goleak"
)

const CONN_ADDR_RMQ_LOCAL = "amqp://guest:guest@localhost:5672/"
const CONTAINER_ENGINE = "podman" // used docker if that's your setup

func startRabbitEngine() (string, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// podman run --rm -d -p 5672:5672 -p 15672:15672 --name some-rabbit
	out, err := exec.CommandContext(ctx,
		CONTAINER_ENGINE, "run", "--rm", "-d",
		"-p=5672:5672", "-p=15672:15672", "--name", "grabbit-test-rmq-engine",
		"--quiet", "rabbitmq:management").Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func stopRabbitEngine(containerId string) error {
	if err := exec.Command(CONTAINER_ENGINE, "rm", "--force", containerId).Run(); err != nil {
		return err
	}
	return nil
}

func TestMain(m *testing.M) {
	containerId, err := startRabbitEngine()
	if err != nil {
		log.Fatalf("failed to start RMQ engine: %v", err)
	}
	log.Printf("RMQ container started: %s", containerId)
	flag.Parse() // capture things like '-race', etc.
	// FIXME perhaps adopt leak detection at individual test level
	goleak.VerifyTestMain(m, goleak.Cleanup(func(exitCode int) {
		if err := stopRabbitEngine(containerId); err != nil {
			log.Fatalf("failed to stop RMQ engine: %v", err)
		}
		log.Printf("RMQ container stopped: %s", containerId)
	}))
}

// wgDoneOrTimeout waits till the 'wg' is done (true) or timeout expires (false)
func wgDoneOrTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		wg.Wait()
		close(c)
	}()
	select {
	case <-c:
		return true
	case <-time.After(timeout):
		wg.Done() // N.B. this still leaks the coroutine if wg was > 1
		return false
	}
}

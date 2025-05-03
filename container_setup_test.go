package grabbit

import (
	"context"
	"log"
	"math/rand"
	"os"
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
	skip_container := os.Getenv("TEST_SKIP_CONTAINER")
	var containerId string
	var err error

	if len(skip_container) == 0 {
		containerId, err = startRabbitEngine()
		if err != nil {
			log.Fatalf("failed to start RMQ engine: %v", err)
		}
		log.Printf("RMQ container started: %s", containerId)
	}

	flag.Parse() // capture things like '-race', etc.
	// FIXME perhaps adopt leak detection at individual test level
	goleak.VerifyTestMain(m, goleak.Cleanup(func(exitCode int) {
		if len(skip_container) == 0 {
			if err := stopRabbitEngine(containerId); err != nil {
				log.Fatalf("failed to stop RMQ engine (%s): %v", containerId, err)
			}
			log.Printf("RMQ container stopped: %s", containerId)
		}
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

// ConditionWait tests for the 'cond' condition until it happens (true) or timeout expires (false).
// The tested condition should not block, i.e. wg.Wait() is not a good candidate. Ideally the
// inner functionality should be fast and concurrency safe.
func ConditionWait(ctx context.Context, cond func() bool, timeout, pollFreq time.Duration) bool {
	if timeout == 0 {
		timeout = 7500 * time.Millisecond
	}
	if pollFreq == 0 {
		pollFreq = 330 * time.Millisecond
	}

	d := time.Now().Add(timeout)
	ctxLocal, cancel := context.WithDeadline(ctx, d)
	defer cancel()

	// status polling
	ticker := time.NewTicker(pollFreq)
	defer ticker.Stop()

	for {
		select {
		case <-ctxLocal.Done():
			return false
		case <-ticker.C:
			if cond() {
				return true
			}
		}
	}
}

type SafeRand struct {
	mu sync.RWMutex
	r  *rand.Rand
}

func NewSafeRand(seed int64) *SafeRand {
	return &SafeRand{
		r: rand.New(rand.NewSource(seed)),
	}
}

func (r *SafeRand) Int() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.r.Int()
}

// SafeRegisterMap keeps a record of tags that have been registered with no
// interest in the associated payload.
type SafeRegisterMap struct {
	mu     sync.RWMutex
	values map[string]struct{}
}

func NewSafeRegisterMap() *SafeRegisterMap {
	return &SafeRegisterMap{
		values: make(map[string]struct{}),
	}
}

func (m *SafeRegisterMap) Set(tag string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.values[tag] = struct{}{}
}

func (m *SafeRegisterMap) Has(tag string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, has := m.values[tag]
	return has
}

func (m *SafeRegisterMap) Length() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.values)
}

// SafeCounter implements a poor man's semaphore
type SafeCounter struct {
	counter       int
	edgeTriggered bool
	mu            sync.RWMutex
}

func (c *SafeCounter) Add(delta int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counter += delta
	c.edgeTriggered = true
}

func (c *SafeCounter) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counter = 0
	c.edgeTriggered = false
}

func (c *SafeCounter) Value() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.counter
}

func (c *SafeCounter) IsZero() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.edgeTriggered && c.counter == 0
}

func (c *SafeCounter) NotZero() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.edgeTriggered && c.counter != 0
}

func (c *SafeCounter) IsDefault() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return !c.edgeTriggered
}

type EventCounters struct {
	Up            *SafeCounter // up = connected
	Down          *SafeCounter // down = disconnected
	Closed        *SafeCounter // closed
	Recovery      *SafeCounter // performing initial or recovery reconnection
	DataExhausted *SafeCounter // consumer has no more data
	MsgPublished  *SafeCounter // messages published
}

// procStatusEvents is a coroutine that processes the connection status events.
// It increments the appropriate counter when an event is received and decrements 'cbRecoveryCounter'
// in order to tally-up with the callbacks (capture just the real hiccups).
//
// Note: sudden death via ctx cancellation _might_ not provide any Down/Closed feedback
func procStatusEvents(
	ctx context.Context,
	chEvents chan Event,
	eventCounters *EventCounters,
	cbRecoveryCounter *SafeCounter,
) {
	for {
		select {
		case <-ctx.Done(): // 1st level of coroutine protection
			return
		case event, ok := <-chEvents:
			if !ok { // 2nd level of coroutine protection
				return
			}
			// these tend to get noisy in our testing
			// if event.Kind != EventMessagePublished {
			// 	log.Printf("event channel %s: %s", event.SourceName, event.Kind)
			// }
			switch event.Kind {
			case EventUp:
				if eventCounters.Up != nil {
					eventCounters.Up.Add(1)
				}
			case EventDown:
				if eventCounters.Down != nil {
					eventCounters.Down.Add(1)
				}
			case EventClosed:
				if eventCounters.Closed != nil {
					eventCounters.Closed.Add(1)
				}
			case EventCannotEstablish:
				if eventCounters.Recovery != nil {
					eventCounters.Recovery.Add(1)
				}
				if cbRecoveryCounter != nil {
					cbRecoveryCounter.Add(-1)
				}
			case EventDataExhausted:
				if eventCounters.DataExhausted != nil {
					eventCounters.DataExhausted.Add(1)
				}
			case EventMessagePublished:
				if eventCounters.MsgPublished != nil {
					eventCounters.MsgPublished.Add(1)
				}
			default:
			}
		}
	}
}

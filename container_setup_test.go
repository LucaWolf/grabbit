package grabbit

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"flag"

	rabbithole "github.com/michaelklishin/rabbit-hole/v3"
	"go.uber.org/goleak"
)

const CONN_ADDR_RMQ_LOCAL = "amqp://guest:guest@localhost:5672/"
const CONN_ADDR_RMQ_REJECT_PWD = "amqp://guest:bad_pwd@localhost:5672/"
const CONTAINER_ENGINE = "podman" // used docker if that's your setup

// RMQC is a globaal external RabbitEngine client for interfacing with the test environment
type RMQC struct {
	Cli *rabbithole.Client
}

var rmqc = RMQC{}

func (r RMQC) awaitRabbitEngine(timeout time.Duration) error {
	var nd []rabbithole.NodeInfo
	var err error

	rmqc.Cli, err = rabbithole.NewClient("http://127.0.0.1:15672", "guest", "guest")
	if err != nil {
		return fmt.Errorf("RMQ controller: new client %w", err)
	}

	delay := 2000 * time.Millisecond
	maxRetries := int(timeout / delay)

	for range maxRetries {
		nd, err = rmqc.Cli.ListNodes()
		if err == nil {
			log.Println("RMQ engine: node ", nd[0].Name)
			return nil
		}
		time.Sleep(delay)

	}
	return fmt.Errorf("RMQ controller: node info %w", err)
}

func (r RMQC) killConnections() error {
	// for some reason listing the connections may fail early, try several times
	var xs []rabbithole.ConnectionInfo
	var err error
	delayer := NewDefaultDelayer()

	for i := range 10 {
		xs, err = rmqc.Cli.ListConnections()
		if err != nil {
			return fmt.Errorf("RMQ controller: list connections %w", err)
		}
		if len(xs) != 0 {
			log.Println("INFO: RMQ controller listed connections on attempt", i)
			break
		}
		<-time.After(delayer.Delay(i))
	}
	// still no luck?
	if len(xs) == 0 {
		return errors.New("RMQ controller: no connections")
	}

	for _, x := range xs {
		rsp, err := rmqc.Cli.CloseConnection(x.Name)
		if err != nil {
			return fmt.Errorf("RMQ controller: close connection %w (%s - %s)", err, x.Name, rsp.Status)
		}
	}
	return nil
}

func (r RMQC) expectQueue(name string) error {
	qs, err := r.Cli.ListQueues()
	if err != nil {
		return err
	}
	if len(qs) == 0 {
		return errors.New("RMQ controller: cannot list queues")
	}
	for _, q := range qs {
		if q.Name == name {
			return nil
		}
	}
	return fmt.Errorf("queue %s not found", name)
}

func (r RMQC) expectExchange(name string) error {
	es, err := r.Cli.ListExchanges()
	if err != nil {
		return err
	}
	if len(es) == 0 {
		return errors.New("RMQ controller: cannot list exchanges")
	}
	for _, e := range es {
		if e.Name == name {
			return nil
		}
	}
	return fmt.Errorf("exchange %s not found", name)
}

func (r RMQC) startRabbitEngine() (string, error) {
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

func (r RMQC) stopRabbitEngine(containerId string) error {
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
		containerId, err = rmqc.startRabbitEngine()
		if err != nil {
			log.Fatalf("failed to start RMQ engine: %v", err)
		}
		log.Printf("RMQ container started: %s", containerId)
	}
	// now wait for RMQ engine to be available
	tStart := time.Now()
	if rmqc.awaitRabbitEngine(20*time.Second) != nil {
		if len(skip_container) == 0 {
			rmqc.stopRabbitEngine(containerId)
		}
		log.Fatal("RabbitMQ engine not available")
	}
	log.Printf("RMQ engine operational (%v). Start the actual testing...\n", time.Since(tStart))

	flag.Parse() // capture things like '-race', etc.
	// FIXME perhaps adopt leak detection at individual test level
	goleak.VerifyTestMain(m, goleak.Cleanup(func(exitCode int) {
		if len(skip_container) == 0 {
			if err := rmqc.stopRabbitEngine(containerId); err != nil {
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

// ConditionWait tests for the `cond` condition until it happens (true) or timeout expires (false).
// Zero values for `timeout` and `pollFreq` defaults to 7.5 sec and 330 ms respectively.
// The tested condition should not block, i.e. wg.Wait() is not a good candidate. Ideally the
// inner functionality should be fast and concurrency safe.
func ConditionWait(ctx context.Context, cond func() bool, poll EvtPollingPolicy) bool {
	d := time.Now().Add(poll.Timeout)
	ctxLocal, cancel := context.WithDeadline(ctx, d)
	defer cancel()

	// status polling
	ticker := time.NewTicker(poll.Frequency)
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

func (r *SafeRand) ClampInt(upper int64) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.r.Int63n(upper)
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

func (m *SafeRegisterMap) IsZero() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.values) == 0
}

func (m *SafeRegisterMap) ValueEquals(value int) func() bool {
	return func() bool {
		m.mu.RLock()
		defer m.mu.RUnlock()
		return len(m.values) == value
	}
}

func (m *SafeRegisterMap) Greater(value int) func() bool {
	return func() bool {
		m.mu.RLock()
		defer m.mu.RUnlock()
		return len(m.values) > value
	}
}

func (m *SafeRegisterMap) GreaterEquals(value int) func() bool {
	return func() bool {
		m.mu.RLock()
		defer m.mu.RUnlock()
		return len(m.values) >= value
	}
}

func (m *SafeRegisterMap) Less(value int) func() bool {
	return func() bool {
		m.mu.RLock()
		defer m.mu.RUnlock()
		return len(m.values) < value
	}
}

func (m *SafeRegisterMap) LessEquals(value int) func() bool {
	return func() bool {
		m.mu.RLock()
		defer m.mu.RUnlock()
		return len(m.values) <= value
	}
}

func (m *SafeRegisterMap) NotZero() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.values) != 0
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

func (c *SafeCounter) ValueEquals(value int) func() bool {
	return func() bool {
		c.mu.RLock()
		defer c.mu.RUnlock()
		return c.edgeTriggered && c.counter == value
	}
}

func (c *SafeCounter) Greater(value int) func() bool {
	return func() bool {
		c.mu.RLock()
		defer c.mu.RUnlock()
		return c.edgeTriggered && c.counter > value
	}
}

func (c *SafeCounter) GreaterEquals(value int) func() bool {
	return func() bool {
		c.mu.RLock()
		defer c.mu.RUnlock()
		return c.edgeTriggered && c.counter >= value
	}
}

func (c *SafeCounter) Less(value int) func() bool {
	return func() bool {
		c.mu.RLock()
		defer c.mu.RUnlock()
		return c.edgeTriggered && c.counter < value
	}
}

func (c *SafeCounter) LessEquals(value int) func() bool {
	return func() bool {
		c.mu.RLock()
		defer c.mu.RUnlock()
		return c.edgeTriggered && c.counter <= value
	}
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
	Up            *SafeCounter // EventUp
	Down          *SafeCounter // EventDown
	Closed        *SafeCounter // EventClosed
	BadRecovery   *SafeCounter // EventCannotEstablish
	DataExhausted *SafeCounter // EventDataExhausted
	MsgPublished  *SafeCounter // EventMessagePublished
	MsgReceived   *SafeCounter // EventMessageReceived
	Topology      *SafeCounter // EventDefineTopology
}

// evtAny returns the logical OR of the provides events (usually SafeCounter list)
func evtAny(funcs ...func() bool) func() bool {
	return func() bool {
		for _, f := range funcs {
			if f() {
				return true
			}
		}
		return false
	}
}

// evtAll returns logical AND of the provided events (usually SafeCounter list).
func evtAll(funcs ...func() bool) func() bool {
	return func() bool {
		for _, f := range funcs {
			if !f() {
				return false
			}
		}
		return true
	}
}

func evtNot(f func() bool) func() bool {
	return func() bool {
		return !f()
	}
}

var downCallbackCounter SafeCounter
var upCallbackCounter SafeCounter
var recoveringCallbackCounter SafeCounter
var delayerCallbackCounter SafeCounter
var pwdCallbackCounter SafeCounter

func connDownCB(name string, err OptionalError) bool {
	downCallbackCounter.Add(1)
	return true // want continuing
}

func connUpCB(name string) {
	upCallbackCounter.Add(1)
}

func connReconnectCB(name string, retry int) bool {
	recoveringCallbackCounter.Add(1)
	return true // want continuing
}

type tracingDelayer struct {
	Value time.Duration
}

// Delay implements the DelayProvider i/face for the DefaultDelayer.
func (delayer tracingDelayer) Delay(retry int) time.Duration {
	delayerCallbackCounter.Add(1)
	return delayer.Value
}

type pwdProvider struct {
	Value string
}

func (p pwdProvider) Password() (string, error) {
	pwdCallbackCounter.Add(1)
	return p.Value, nil
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
				if eventCounters.BadRecovery != nil {
					eventCounters.BadRecovery.Add(1)
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
			case EventMessageReceived:
				if eventCounters.MsgReceived != nil {
					eventCounters.MsgReceived.Add(1)
				}
			case EventDefineTopology:
				if eventCounters.Topology != nil {
					eventCounters.Topology.Add(1)
				}
			default:
				if event.Err.IsSet() {
					log.Printf("event channel %s: %s: %s: %s",
						event.SourceName, event.TargetName, event.Kind, event.Err)
				}
			}
		}
	}
}

type EvtPollingPolicy struct {
	Timeout   time.Duration // total duration
	Frequency time.Duration // interim check points for testing condition
}

var DefaultPoll = EvtPollingPolicy{
	Timeout:   7500 * time.Millisecond,
	Frequency: 330 * time.Millisecond,
}

var ShortPoll = EvtPollingPolicy{
	Timeout:   3 * time.Second,
	Frequency: 330 * time.Millisecond,
}

var LongPoll = EvtPollingPolicy{
	Timeout:   30 * time.Second,
	Frequency: 1 * time.Second,
}

var LongVeryFrequentPoll = EvtPollingPolicy{
	Timeout:   30 * time.Second,
	Frequency: 50 * time.Millisecond, // orig was 30
}

func TestEventsAny(t *testing.T) {
	tests := []struct {
		funcs []func() bool
		want  bool
	}{
		{
			funcs: []func() bool{
				func() bool { return false },
				func() bool { return false },
				func() bool { return false },
			},
			want: false,
		},
		{
			funcs: []func() bool{
				func() bool { return false },
				func() bool { return true },
				func() bool { return false },
			},
			want: true,
		},
		{
			funcs: []func() bool{
				func() bool { return true },
				func() bool { return true },
				func() bool { return true },
			},
			want: true,
		},
		{
			funcs: []func() bool{},
			want:  false,
		},
	}

	for _, tt := range tests {
		got := evtAny(tt.funcs...)
		result := got()
		if result != tt.want {
			t.Errorf("condOR() = %v, want %v", result, tt.want)
		}
	}
}

func TestEventsAll(t *testing.T) {
	tests := []struct {
		funcs []func() bool
		want  bool
	}{
		{
			funcs: []func() bool{
				func() bool { return false },
				func() bool { return false },
				func() bool { return false },
			},
			want: false,
		},
		{
			funcs: []func() bool{
				func() bool { return false },
				func() bool { return true },
				func() bool { return false },
			},
			want: false,
		},
		{
			funcs: []func() bool{
				func() bool { return true },
				func() bool { return true },
				func() bool { return true },
			},
			want: true,
		},
		{
			funcs: []func() bool{},
			want:  true,
		},
	}

	for _, tt := range tests {
		got := evtAll(tt.funcs...)
		result := got()
		if result != tt.want {
			t.Errorf("condOR() = %v, want %v", result, tt.want)
		}
	}
}

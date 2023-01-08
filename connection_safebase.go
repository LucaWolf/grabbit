package grabbit

import (
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// SafeBaseConn wraps in a concurrency safe way the low level amqp.Connection.
type SafeBaseConn struct {
	super *amqp.Connection // core connection
	mu    sync.RWMutex     // makes this concurrent safe, maintenance wise only!
}

// IsSet tests if the low level amqp connection is set.
func (c *SafeBaseConn) IsSet() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.super != nil
}

// Super returns the low level amqp connection for direct interactions.
// Use sparingly and prefer using the predefined [Connection] wrapping methods instead.
// Pair usage with the locking/unlocking routines for safety!
func (c *SafeBaseConn) Super() *amqp.Connection {
	return c.super
}

// Lock acquires locking of the low level connection [Super] for amqp operations.
// Use sparingly and fast as this locks-out the channel recovery!
func (c *SafeBaseConn) Lock() {
	c.mu.Lock()
}

// UnLock releases the low level connection [Super] lock.
func (c *SafeBaseConn) UnLock() {
	c.mu.Unlock()
}

// set is a private method for updating the super connection (post recovery)
func (c *SafeBaseConn) set(super *amqp.Connection) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.super = super
}

// reset is a private method for removing the current super connection (pre-recovery)
func (c *SafeBaseConn) reset() {
	c.set(nil)
}

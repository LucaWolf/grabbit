package grabbit

import (
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// SafeBaseChan wraps in a concurrency safe way the low level amqp.Channel.
type SafeBaseChan struct {
	super *amqp.Channel // core channel
	mu    sync.RWMutex  // makes this concurrent safe
}

// IsSet checks whether the SafeBaseChan's super field is set.
//
// It does this by acquiring a read lock on the SafeBaseChan's mutex
// and then deferring its release.
//
// Returns true if the super field is not nil, false otherwise.
func (c *SafeBaseChan) IsSet() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.super != nil
}

// Super returns the low level amqp channel for direct interactions.
// Use sparingly and prefer using the predefined [Channel] wrapping methods instead.
// Pair usage with the locking/unlocking routines for safety!
func (c *SafeBaseChan) Super() *amqp.Channel {
	return c.super
}

// Lock acquires locking of the low level channel [Super] for amqp operations.
// Use sparingly and fast as this locks-out the channel recovery!
func (c *SafeBaseChan) Lock() {
	c.mu.Lock()
}

// UnLock releases the low level channel [Super] lock.
func (c *SafeBaseChan) UnLock() {
	c.mu.Unlock()
}

// RLock acquires read locking of the low level channel [Super] for amqp operations.
// Use sparingly and fast as this locks-out the channel recovery!
func (c *SafeBaseChan) RLock() {
	c.mu.Lock()
}

// RUnlock releases the low level channel [Super] read lock.
func (c *SafeBaseChan) RUnlock() {
	c.mu.Unlock()
}

// set is a private method for updating the super channel (post recovery)
func (c *SafeBaseChan) set(super *amqp.Channel) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.super = super
}

// reset is a private method for removing the current super channel (pre-recovery)
func (c *SafeBaseChan) reset() {
	c.set(nil)
}

package grabbit

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

// IsBlocked returns the TCP flow status of the base connection.
func (conn *Connection) IsBlocked() bool {
	return conn.blocked.Locked()
}

// IsClosed safely wraps the amqp connection IsClosed
func (conn *Connection) IsClosed() bool {
	conn.baseConn.mu.RLock()
	defer conn.baseConn.mu.RUnlock()

	return conn.baseConn.super == nil || conn.baseConn.super.IsClosed()
}

// Close safely wraps the amqp connection Close and terminates the maintenance loop.
// The inner base connection is reset and the context is cancelled.
func (conn *Connection) Close() error {
	conn.baseConn.mu.Lock()
	defer conn.baseConn.mu.Unlock()

	var err error

	if conn.baseConn.super != nil {
		err = conn.baseConn.super.Close()
		conn.baseConn.super = nil
	}
	conn.opt.cancelCtx()

	return err
}

// Channel safely wraps the amqp connection Channel() function.
func (conn *Connection) Channel() (*amqp.Channel, error) {
	conn.baseConn.mu.Lock()
	defer conn.baseConn.mu.Unlock()

	if conn.baseConn.super != nil {
		return conn.baseConn.super.Channel()
	}

	return nil, amqp.ErrClosed
}

// Connection returns the safe base connection and thus indirectly the low level library connection.
func (conn *Connection) Connection() *SafeBaseConn {
	return &conn.baseConn
}

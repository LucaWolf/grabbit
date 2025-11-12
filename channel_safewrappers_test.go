package grabbit

import (
	"context"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestChannel_IsClosed(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-is-closed"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	if ch.IsClosed() {
		t.Errorf("IsClosed() should return false when channel is open")
	}
}

func TestChannel_Close(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-close"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	err := ch.Close()
	if err != nil {
		t.Errorf("Close() should not return an error: %v", err)
	}

	if !ch.IsClosed() {
		t.Errorf("IsClosed() should return true after Close()")
	}
}

func TestChannel_Cancel(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-cancel"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	consumer := "test-consumer"
	noWait := false

	_, err := ch.QueueDeclare("test-queue", false, false, false, noWait, nil)
	if err != nil {
		t.Fatalf("QueueDeclare() failed: %v", err)
	}

	_, err = ch.Consume("test-queue", consumer, true, false, false, noWait, nil)
	if err != nil {
		t.Fatalf("Consume() failed: %v", err)
	}

	err = ch.Cancel(consumer, noWait)
	if err != nil {
		t.Errorf("Cancel() should not return an error: %v", err)
	}
}

func TestChannel_Reject(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-reject"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	deliveryTag := uint64(1)
	requeue := false

	err := ch.Reject(deliveryTag, requeue)
	if err != nil {
		t.Errorf("Reject() should not return an error: %v", err)
	}
}

func TestChannel_Ack(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-ack"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	deliveryTag := uint64(1)
	multiple := false

	err := ch.Ack(deliveryTag, multiple)
	if err != nil {
		t.Errorf("Ack() should not return an error: %v", err)
	}
}

func TestChannel_Nack(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-nack"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	deliveryTag := uint64(1)
	multiple := false
	requeue := false

	err := ch.Nack(deliveryTag, multiple, requeue)
	if err != nil {
		t.Errorf("Nack() should not return an error: %v", err)
	}
}

func TestChannel_QueueInspect(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-queue-inspect"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	queueName := "test-queue"

	_, err := ch.QueueDeclare("test-queue", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare() failed: %v", err)
	}

	_, err = ch.QueueInspect(queueName)
	if err != nil {
		t.Errorf("QueueInspect() should not return an error: %v", err)
	}
}

func TestChannel_QueueDeclarePassive(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-queue-declare-passive"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	queueName := "test-queue"
	durable := false
	autoDelete := false
	exclusive := false
	noWait := false
	args := amqp.Table{}

	_, err := ch.QueueDeclare(queueName, durable, autoDelete, exclusive, noWait, args)
	if err != nil {
		t.Fatalf("QueueDeclare() failed: %v", err)
	}

	_, err = ch.QueueDeclarePassive(queueName, durable, autoDelete, exclusive, noWait, args)
	if err != nil {
		t.Errorf("QueueDeclarePassive() should not return an error: %v", err)
	}
}

func TestChannel_PublishWithContext(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-publish-with-context"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	ctx := context.Background()
	exchange := "test-exchange"
	key := "test-key"
	mandatory := false
	immediate := false
	msg := amqp.Publishing{}

	err := ch.ExchangeDeclare(exchange, amqp.ExchangeDirect, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("ExchangeDeclare() failed: %v", err)
	}

	err = ch.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
	if err != nil {
		t.Errorf("PublishWithContext() should not return an error: %v", err)
	}
}

func TestChannel_PublishWithDeferredConfirmWithContext(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-publish-with-deferred-confirm-with-context"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	ctx := context.Background()
	exchange := "test-exchange"
	key := "test-key"
	mandatory := false
	immediate := false
	msg := amqp.Publishing{}

	err := ch.ExchangeDeclare(exchange, amqp.ExchangeDirect, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("ExchangeDeclare() failed: %v", err)
	}

	_, err = ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
	if err != nil {
		t.Errorf("PublishWithDeferredConfirmWithContext() should not return an error: %v", err)
	}
}

func TestChannel_QueuePurge(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-queue-purge"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	queueName := "test-queue"
	noWait := false

	_, err := ch.QueueDeclare(queueName, false, false, false, noWait, nil)
	if err != nil {
		t.Fatalf("QueueDeclare() failed: %v", err)
	}

	_, err = ch.QueuePurge(queueName, noWait)
	if err != nil {
		t.Errorf("QueuePurge() should not return an error: %v", err)
	}
}

func TestChannel_GetNextPublishSeqNo(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-get-next-publish-seq-no"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	seqNo := ch.GetNextPublishSeqNo()
	if seqNo != uint64(1) {
		t.Errorf("GetNextPublishSeqNo() should return 1, got %d", seqNo)
	}
}

func TestChannel_QueueDelete(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-queue-delete"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	queueName := "test-queue"
	ifUnused := false
	ifEmpty := false
	noWait := false

	_, err := ch.QueueDeclare(queueName, false, false, false, noWait, nil)
	if err != nil {
		t.Fatalf("QueueDeclare() failed: %v", err)
	}

	_, err = ch.QueueDelete(queueName, ifUnused, ifEmpty, noWait)
	if err != nil {
		t.Errorf("QueueDelete() should not return an error: %v", err)
	}
}

func TestChannel_QueueDeclare(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-queue-declare"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	queueName := "test-queue"
	durable := false
	autoDelete := false
	exclusive := false
	noWait := false
	args := amqp.Table{}

	_, err := ch.QueueDeclare(queueName, durable, autoDelete, exclusive, noWait, args)
	if err != nil {
		t.Errorf("QueueDeclare() should not return an error: %v", err)
	}
}

func TestChannel_ExchangeDeclare(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-exchange-declare"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	exchangeName := "test-exchange"
	exchangeKind := amqp.ExchangeDirect
	durable := false
	autoDelete := false
	internal := false
	noWait := false
	args := amqp.Table{}

	err := ch.ExchangeDeclare(exchangeName, exchangeKind, durable, autoDelete, internal, noWait, args)
	if err != nil {
		t.Errorf("ExchangeDeclare() should not return an error: %v", err)
	}
}

func TestChannel_Qos(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-qos"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	prefetchCount := 10
	prefetchSize := 0 // server(s) not supporting non-zero sizes
	global := false

	err := ch.Qos(prefetchCount, prefetchSize, global)
	if err != nil {
		t.Errorf("Qos() should not return an error: %v", err)
	}
}

func TestChannel_Consume(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-consume"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	queueName := "test-queue"
	consumer := "test-consumer"
	autoAck := true
	exclusive := false
	noLocal := false
	noWait := false
	args := amqp.Table{}

	_, err := ch.QueueDeclare(queueName, false, false, false, noWait, nil)
	if err != nil {
		t.Fatalf("QueueDeclare() failed: %v", err)
	}

	_, err = ch.Consume(queueName, consumer, autoAck, exclusive, noLocal, noWait, args)
	if err != nil {
		t.Errorf("Consume() should not return an error: %v", err)
	}
}

func TestChannel_QueueDeclareWithTopology(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-queue-declare-with-topology"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	topology := &TopologyOptions{
		Name:          "test-queue",
		Durable:       false,
		AutoDelete:    false,
		Exclusive:     false,
		NoWait:        false,
		Args:          nil,
		IsDestination: true,
	}

	q, err := ch.QueueDeclareWithTopology(topology)
	if err != nil {
		t.Errorf("QueueDeclareWithTopology() should not return an error: %v", err)
	}

	if q.Name != topology.Name {
		t.Errorf("QueueDeclareWithTopology() wrong queue name: %s vs. %s", q.Name, topology.Name)
	}
}

func TestChannel_ExchangeDeclareWithTopology(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-exchange-declare-with-topology"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	topology := &TopologyOptions{
		Name:       "test-exchange",
		Kind:       amqp.ExchangeDirect,
		Durable:    false,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Args:       nil,
		Declare:    true,
		IsExchange: true,
	}

	err := ch.ExchangeDeclareWithTopology(topology)
	if err != nil {
		t.Errorf("ExchangeDeclareWithTopology() should not return an error: %v", err)
	}
}

func TestChannel_Channel(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-channel"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	if ch.Channel() == nil {
		t.Errorf("Channel() should not return nil")
	}
}

func TestChannel_NotifyClose(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-notify-close"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	c := make(chan *amqp.Error, 1)
	notifyClose := ch.NotifyClose(c)

	if notifyClose != c {
		t.Errorf("NotifyClose() should return the same channel")
	}
}

func TestChannel_NotifyFlow(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-notify-flow"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	c := make(chan bool, 1)
	notifyFlow := ch.NotifyFlow(c)

	if notifyFlow != c {
		t.Errorf("NotifyFlow() should return the same channel")
	}
}

func TestChannel_NotifyReturn(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-notify-return"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	c := make(chan amqp.Return, 1)
	notifyReturn := ch.NotifyReturn(c)

	if notifyReturn != c {
		t.Errorf("NotifyReturn() should return the same channel")
	}
}

func TestChannel_NotifyCancel(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-notify-cancel"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	c := make(chan string, 1)
	notifyCancel := ch.NotifyCancel(c)

	if notifyCancel != c {
		t.Errorf("NotifyCancel() should return the same channel")
	}
}

func TestChannel_NotifyConfirm(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-notify-confirm"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	ack := make(chan uint64, 1)
	nack := make(chan uint64, 1)
	returnedAck, returnedNack := ch.NotifyConfirm(ack, nack)

	if returnedAck != ack {
		t.Errorf("NotifyConfirm() should return the same ack channel")
	}

	if returnedNack != nack {
		t.Errorf("NotifyConfirm() should return the same nack channel")
	}
}

func TestChannel_NotifyPublish(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-notify-publish"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	confirm := make(chan amqp.Confirmation, 1)
	notifyPublish := ch.NotifyPublish(confirm)

	if notifyPublish != confirm {
		t.Errorf("NotifyPublish() should return the same channel")
	}
}

func TestChannel_QueueBind(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-queue-bind"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	queueName := "test-queue"
	exchangeName := "test-exchange"
	routingKey := "test-routing-key"
	noWait := false
	args := amqp.Table{}

	err := ch.ExchangeDeclare(exchangeName, amqp.ExchangeDirect, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("ExchangeDeclare() failed: %v", err)
	}

	_, err = ch.QueueDeclare(queueName, false, false, false, noWait, nil)
	if err != nil {
		t.Fatalf("QueueDeclare() failed: %v", err)
	}

	err = ch.QueueBind(queueName, routingKey, exchangeName, noWait, args)
	if err != nil {
		t.Errorf("QueueBind() should not return an error: %v", err)
	}
}

func TestChannel_ConsumeWithContext(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-consume-with-context"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	queueName := "test-queue"
	consumer := "test-consumer"
	autoAck := true
	exclusive := false
	noLocal := false
	noWait := false
	args := amqp.Table{}
	ctx := context.Background()

	_, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare() failed: %v", err)
	}

	_, err = ch.ConsumeWithContext(ctx, queueName, consumer, autoAck, exclusive, noLocal, noWait, args)
	if err != nil {
		t.Errorf("ConsumeWithContext() should not return an error: %v", err)
	}
}

func TestChannel_Publish(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-publish"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	exchangeName := "test-exchange"
	routingKey := "test-routing-key"
	mandatory := false
	immediate := false
	msg := amqp.Publishing{}

	err := ch.ExchangeDeclare(exchangeName, amqp.ExchangeDirect, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("ExchangeDeclare() failed: %v", err)
	}

	err = ch.Publish(exchangeName, routingKey, mandatory, immediate, msg)
	if err != nil {
		t.Errorf("Publish() should not return an error: %v", err)
	}
}

func TestChannel_PublishWithDeferredConfirm(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-publish-with-deferred-confirm"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	exchangeName := "test-exchange"
	routingKey := "test-routing-key"
	mandatory := false
	immediate := false
	msg := amqp.Publishing{}

	err := ch.ExchangeDeclare(exchangeName, amqp.ExchangeDirect, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("ExchangeDeclare() failed: %v", err)
	}

	_, err = ch.PublishWithDeferredConfirm(exchangeName, routingKey, mandatory, immediate, msg)
	if err != nil {
		t.Errorf("PublishWithDeferredConfirm() should not return an error: %v", err)
	}
}

func TestChannel_Tx(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-tx"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	err := ch.Tx()
	if err != nil {
		t.Errorf("Tx() should not return an error: %v", err)
	}
}

func TestChannel_TxCommit(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-tx-commit"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	err := ch.Tx()
	if err != nil {
		t.Fatalf("Tx() failed: %v", err)
	}

	err = ch.TxCommit()
	if err != nil {
		t.Errorf("TxCommit() should not return an error: %v", err)
	}
}

func TestChannel_TxRollback(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-tx-rollback"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	err := ch.Tx()
	if err != nil {
		t.Fatalf("Tx() failed: %v", err)
	}

	err = ch.TxRollback()
	if err != nil {
		t.Errorf("TxRollback() should not return an error: %v", err)
	}
}

func TestChannel_Flow(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-flow"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	active := true

	err := ch.Flow(active)
	if err != nil {
		t.Errorf("Flow() should not return an error: %v", err)
	}
}

func TestChannel_Confirm(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-confirm"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	noWait := false

	err := ch.Confirm(noWait)
	if err != nil {
		t.Errorf("Confirm() should not return an error: %v", err)
	}
}

func TestChannel_Recover(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-recover"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	requeue := false

	err := ch.Recover(requeue)
	if err != nil {
		t.Errorf("Recover() should not return an error: %v", err)
	}
}

func TestChannel_Get(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-get"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	queueName := "test-queue"
	autoAck := true

	_, err := ch.QueueDeclare(queueName, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare() failed: %v", err)
	}

	_, _, err = ch.Get(queueName, autoAck)
	if err != nil {
		t.Errorf("Get() should not return an error: %v", err)
	}
}

func TestChannel_QueueUnbind(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-queue-unbind"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	queueName := "test-queue"
	exchangeName := "test-exchange"
	routingKey := "test-routing-key"
	args := amqp.Table{}

	err := ch.ExchangeDeclare(exchangeName, amqp.ExchangeDirect, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("ExchangeDeclare() failed: %v", err)
	}

	_, err = ch.QueueDeclare(queueName, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare() failed: %v", err)
	}

	err = ch.QueueBind(queueName, routingKey, exchangeName, false, args)
	if err != nil {
		t.Fatalf("QueueBind() failed: %v", err)
	}

	err = ch.QueueUnbind(queueName, routingKey, exchangeName, args)
	if err != nil {
		t.Errorf("QueueUnbind() should not return an error: %v", err)
	}
}

func TestChannel_Name(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-name"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	channelName := "test-channel"

	ch := NewChannel(conn, WithChannelOptionName(channelName))
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	if ch.Name() != channelName {
		t.Errorf("Name() should return %s, got %s", channelName, ch.Name())
	}
}

func TestChannel_ExchangeDelete(t *testing.T) {
	conn := NewConnection(CONN_ADDR_RMQ_LOCAL, amqp.Config{}, WithConnectionOptionName("test-connection-exchange-delete"))
	if conn == nil {
		t.Fatalf("Failed to create connection")
	}
	defer AwaitConnectionManagerDone(conn)
	defer conn.Close()

	ch := NewChannel(conn)
	defer ch.Close()
	ch.AwaitStatus(true, DefaultPoll.Timeout)

	exchangeName := "test-exchange"
	ifUnused := false
	noWait := false

	err := ch.ExchangeDeclare(exchangeName, amqp.ExchangeDirect, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("ExchangeDeclare() failed: %v", err)
	}

	err = ch.ExchangeDelete(exchangeName, ifUnused, noWait)
	if err != nil {
		t.Errorf("ExchangeDelete() should not return an error: %v", err)
	}
}

// FIXME create a super test with to goroutines, each creating a channle of a master connection
// then loop publishing and consumer. Sprinkle publishing calls with various calls like isConnected and
// queue probing. The consumer calls randomly ACK or NAK with true or false for re-enqueuing the messages.
// publsiher lop exits after couple of hundred messages.

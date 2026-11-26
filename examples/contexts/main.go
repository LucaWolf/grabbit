package main

import (
	"context"
	"log"
	"time"

	grabbit "github.com/LucaWolf/grabbit"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn := grabbit.NewConnection(
		"amqp://guest:guest@localhost", amqp.Config{},
		grabbit.WithConnectionOptionName("conn.main"),
	)

	alphaStatusChan := make(chan grabbit.Event, 5)
	ctxAlpha, ctxAlphaCancel := context.WithCancel(context.TODO())

	ctxBeta, ctxBetaCancel := context.WithCancel(ctxAlpha)
	betaStatusChan := make(chan grabbit.Event, 5)

	alphaCh := grabbit.NewChannel(conn,
		grabbit.WithChannelOptionContext(ctxAlpha),
		grabbit.WithChannelOptionName("chan.alpha"),
		grabbit.WithChannelOptionNotification(alphaStatusChan),
	)
	betaCh := grabbit.NewChannel(conn,
		grabbit.WithChannelOptionContext(ctxBeta),
		grabbit.WithChannelOptionName("chan.beta"),
		grabbit.WithChannelOptionNotification(betaStatusChan),
	)

	chSignalBetaUp := make(chan struct{})
	chSignalAlphaUp := make(chan struct{})

	// WARN: sudden death via ctx cancellation will not provide any EventClosed feedback;
	// the closed state is observed with Channel.AwaitStatus(false, ...) below.
	go func() {
		for event := range betaStatusChan {
			log.Print("beta.notification: ", event)
			if event.Kind == grabbit.EventUp {
				close(chSignalBetaUp)
			}
		}
	}()
	go func() {
		for event := range alphaStatusChan {
			log.Print("alfa.notification: ", event)
			if event.Kind == grabbit.EventUp {
				close(chSignalAlphaUp)
			}
		}
	}()

	// closing alphaCh should not close betaCh even though beta has been initiate from a child context
	// this is because closing alphaCh will trigger the hidden ctxInnerAlpha, also child of ctxAlpha.
	// So ctxInnerAlpha and ctxBeta are siblings off ctxAlphaCancel
	<-chSignalAlphaUp
	<-chSignalBetaUp
	<-time.After(1 * time.Second)
	alphaCh.Close()

	if betaCh.AwaitStatus(false, 7*time.Second) {
		log.Fatal("Error: closing alphaCh should not have affected betaCh")
	}

	betaStatusTest := "failed"
	if !betaCh.IsClosed() {
		betaStatusTest = "pass"
	}
	log.Println("1. chan.beta status test:", betaStatusTest)

	// instead closing the parent context ctxAlpha should induce beta to shut-down
	ctxAlphaCancel()
	if !betaCh.AwaitStatus(false, 7*time.Second) {
		log.Fatal("Error: cancelling ctxAlpha should have closed betaCh")
	}

	// all is fine, test we can call shutdowns willy-nilly
	betaStatusTest = "failed"
	if betaCh.IsClosed() {
		betaStatusTest = "pass"
	}
	log.Println("2. chan.beta status test:", betaStatusTest)

	if err := betaCh.Close(); err != nil {
		// this is to be expected, implementation calls on a
		// still valid but closed base level amqp.channel
		log.Println("Expected: cannot close chan.beta", err)
	}
	ctxBetaCancel() // this should have been gone by now
	log.Println("...looking good!")
}

// Command demo runs the example sequence from the brief.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/prettyirrelevant/appendix/broker"
)

func main() {
	out := log.New(os.Stdout, "", log.Lmicroseconds)

	b := broker.New[string]()
	pub := broker.NewPublisher(b)

	publish(pub, 1, 5)

	ctx := context.Background()

	sub1 := broker.NewSubscriber(b, printer(out, "subscriber-1"))
	sub1.Subscribe(ctx)
	if sub1.Subscribe(ctx) {
		out.Fatal("second Subscribe call must be a no-op")
	}

	publish(pub, 6, 10)

	sub2 := broker.NewSubscriber(b, printer(out, "subscriber-2"))
	sub2.Subscribe(ctx)

	b.Close()
	<-sub1.Done()
	<-sub2.Done()

	out.Printf("done: subscriber-1 processed %d, subscriber-2 processed %d",
		sub1.Offset(), sub2.Offset())
}

func publish(pub *broker.Publisher[string], from, to int) {
	for i := from; i <= to; i++ {
		if _, err := pub.Publish(fmt.Sprintf("message-%d", i)); err != nil {
			log.Fatalf("publish message-%d: %v", i, err)
		}
	}
}

func printer(out *log.Logger, name string) func(broker.Message[string]) {
	return func(m broker.Message[string]) {
		out.Printf("%s offset=%d payload=%s", name, m.Offset, m.Payload)
	}
}

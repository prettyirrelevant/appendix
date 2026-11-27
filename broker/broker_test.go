package broker_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/prettyirrelevant/appendix/broker"
)

type recorder[T any] struct {
	mu   sync.Mutex
	msgs []T
}

func (r *recorder[T]) handle(m broker.Message[T]) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, m.Payload)
}

func (r *recorder[T]) got() []T {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.msgs)
}

func publish(t *testing.T, p *broker.Publisher[string], msgs ...string) {
	t.Helper()
	for _, m := range msgs {
		if _, err := p.Publish(m); err != nil {
			t.Fatalf("Publish(%q): %v", m, err)
		}
	}
}

func TestBriefScenario(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := broker.New[string]()
		pub := broker.NewPublisher(b)
		ctx := context.Background()

		publish(t, pub, "m1", "m2", "m3", "m4", "m5")
		var r1, r2 recorder[string]
		sub1 := broker.NewSubscriber(b, r1.handle)
		sub1.Subscribe(ctx)
		publish(t, pub, "m6", "m7", "m8", "m9", "m10")
		sub2 := broker.NewSubscriber(b, r2.handle)
		sub2.Subscribe(ctx)

		b.Close()
		<-sub1.Done()
		<-sub2.Done()

		want := []string{"m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9", "m10"}
		if got := r1.got(); !slices.Equal(got, want) {
			t.Errorf("subscriber 1 got %v, want %v", got, want)
		}
		if got := r2.got(); !slices.Equal(got, want) {
			t.Errorf("subscriber 2 got %v, want %v", got, want)
		}
	})
}

func TestSubscribeTwiceDeliversOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := broker.New[string]()
		publish(t, broker.NewPublisher(b), "a", "b")

		var r recorder[string]
		sub := broker.NewSubscriber(b, r.handle)
		sub.Subscribe(context.Background())
		if sub.Subscribe(context.Background()) {
			t.Error("second Subscribe returned true")
		}
		b.Close()
		<-sub.Done()

		if got := r.got(); !slices.Equal(got, []string{"a", "b"}) {
			t.Errorf("got %v, want [a b]", got)
		}
	})
}

func TestIdleSubscriberWakesOnPublish(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := broker.New[string]()
		var r recorder[string]
		sub := broker.NewSubscriber(b, r.handle)
		sub.Subscribe(context.Background())
		synctest.Wait()

		publish(t, broker.NewPublisher(b), "late")
		synctest.Wait()

		if got := r.got(); !slices.Equal(got, []string{"late"}) {
			t.Errorf("got %v, want [late]", got)
		}
		b.Close()
		<-sub.Done()
	})
}

func TestSlowSubscriberDoesNotBlockOthers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := broker.New[string]()
		ctx := context.Background()

		release := make(chan struct{})
		slow := broker.NewSubscriber(b, func(broker.Message[string]) { <-release })
		var fast recorder[string]
		fastSub := broker.NewSubscriber(b, fast.handle)
		slow.Subscribe(ctx)
		fastSub.Subscribe(ctx)

		pub := broker.NewPublisher(b)
		publish(t, pub, "a", "b", "c")
		synctest.Wait()

		if got := fast.got(); !slices.Equal(got, []string{"a", "b", "c"}) {
			t.Errorf("fast subscriber got %v while slow one was stuck", got)
		}
		if _, err := pub.Publish("d"); err != nil {
			t.Errorf("publisher blocked by slow subscriber: %v", err)
		}

		close(release)
		b.Close()
		<-slow.Done()
		<-fastSub.Done()
	})
}

func TestCancelStopsSubscriber(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := broker.New[string]()
		ctx, cancel := context.WithCancel(context.Background())

		var r recorder[string]
		sub := broker.NewSubscriber(b, r.handle)
		sub.Subscribe(ctx)
		synctest.Wait()
		cancel()
		<-sub.Done()

		publish(t, broker.NewPublisher(b), "after cancel")
		synctest.Wait()
		if got := r.got(); len(got) != 0 {
			t.Errorf("canceled subscriber received %v", got)
		}
	})
}

func TestPublishAfterClose(t *testing.T) {
	b := broker.New[string]()
	b.Close()
	b.Close()
	if _, err := broker.NewPublisher(b).Publish("x"); !errors.Is(err, broker.ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}

// Real goroutines, no synctest, so the race detector sees true parallelism.
func TestConcurrentPublishersAndSubscribers(t *testing.T) {
	const publishers, perPublisher, subscribers = 8, 500, 4

	b := broker.New[string]()
	recs := make([]*recorder[string], subscribers)
	subs := make([]*broker.Subscriber[string], subscribers)
	for i := range subscribers {
		recs[i] = &recorder[string]{}
		subs[i] = broker.NewSubscriber(b, recs[i].handle)
		subs[i].Subscribe(context.Background())
	}

	var wg sync.WaitGroup
	for p := range publishers {
		wg.Go(func() {
			pub := broker.NewPublisher(b)
			for seq := range perPublisher {
				if _, err := pub.Publish(fmt.Sprintf("%d/%d", p, seq)); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	b.Close()
	for _, s := range subs {
		<-s.Done()
	}

	first := recs[0].got()
	if len(first) != publishers*perPublisher {
		t.Fatalf("got %d messages, want %d", len(first), publishers*perPublisher)
	}
	next := make([]int, publishers)
	for _, m := range first {
		var p, seq int
		fmt.Sscanf(m, "%d/%d", &p, &seq)
		if seq != next[p] {
			t.Fatalf("publisher %d: got seq %d, want %d", p, seq, next[p])
		}
		next[p]++
	}
	for i, r := range recs[1:] {
		if !slices.Equal(r.got(), first) {
			t.Errorf("subscriber %d saw a different order from subscriber 0", i+1)
		}
	}
}

// FuzzInterleaving drives a random mix of publishes, new subscriptions, and
// pauses. Whatever the order, every subscriber must end with the whole log.
func FuzzInterleaving(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1}) // the brief's sequence
	f.Add([]byte{1, 2, 0, 2, 1, 0, 0, 2})

	f.Fuzz(func(t *testing.T, ops []byte) {
		synctest.Test(t, func(t *testing.T) {
			b := broker.New[int]()
			pub := broker.NewPublisher(b)
			var (
				published []int
				subs      []*broker.Subscriber[int]
				recs      []*recorder[int]
			)

			for i, op := range ops {
				switch op % 3 {
				case 0:
					if _, err := pub.Publish(i); err != nil {
						t.Fatal(err)
					}
					published = append(published, i)
				case 1:
					r := &recorder[int]{}
					s := broker.NewSubscriber(b, r.handle)
					s.Subscribe(context.Background())
					if s.Subscribe(context.Background()) {
						t.Fatal("second Subscribe returned true")
					}
					subs, recs = append(subs, s), append(recs, r)
				case 2:
					synctest.Wait()
				}
			}

			b.Close()
			for i, s := range subs {
				<-s.Done()
				if got := recs[i].got(); !slices.Equal(got, published) {
					t.Fatalf("subscriber %d got %v, want %v", i, got, published)
				}
			}
		})
	})
}

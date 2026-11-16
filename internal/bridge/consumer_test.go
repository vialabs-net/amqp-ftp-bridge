package bridge

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestDialAMQPCancelHandshake(t *testing.T) {
	for _, scheme := range []string{"amqp", "amqps"} {
		t.Run(scheme, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, _, err := dialAMQP(ctx, scheme+"://"+listener.Addr().String(), amqp.Config{}); done <- err }()
			peer, err := listener.Accept()
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			cancel()
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("handshake succeeded without server response")
				}
			case <-time.After(time.Second):
				t.Fatal("cancelled handshake did not stop")
			}
		})
	}
}

func TestAMQPWriteDeadline(t *testing.T) {
	client, peer := net.Pipe()
	defer client.Close()
	defer peer.Close()
	transport := &amqpTransport{Conn: client, timeout: 20 * time.Millisecond}
	start := time.Now()
	if _, err := transport.Write([]byte("blocked")); err == nil {
		t.Fatal("write succeeded without reader")
	}
	if time.Since(start) > time.Second {
		t.Fatal("write deadline ignored")
	}
}

func TestPublishCancellationBlockedWrite(t *testing.T) {
	client, peer := net.Pipe()
	defer peer.Close()
	defer client.Close()
	serverDone := make(chan error, 1)
	go func() { serverDone <- openTestChannel(peer) }()
	conn, err := amqp.Open(client, amqp.Config{SASL: []amqp.Authentication{&amqp.PlainAuth{Username: "guest", Password: "guest"}}})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.Confirm(false); err != nil {
		t.Fatal(err)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	adapter := newChannel(ch)
	adapter.transport = client
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- adapter.PublishConfirmed(ctx, "dlx", "dlq", amqp.Publishing{Body: make([]byte, 1<<20)})
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("blocked publish was confirmed")
		}
	case <-time.After(time.Second):
		client.Close()
		t.Fatal("publish ignored cancellation")
	}
}

// Minimal broker handshake. After confirm.select-ok it stops reading, so the
// next publish blocks in the transport before waiting for its confirmation.
func openTestChannel(peer net.Conn) error {
	peer.SetDeadline(time.Now().Add(3 * time.Second))
	header := make([]byte, 8)
	if _, err := io.ReadFull(peer, header); err != nil {
		return err
	}
	read := func() error {
		header := make([]byte, 7)
		if _, err := io.ReadFull(peer, header); err != nil {
			return err
		}
		_, err := io.CopyN(io.Discard, peer, int64(binary.BigEndian.Uint32(header[3:]))+1)
		return err
	}
	send := func(channel uint16, payload []byte) error {
		frame := []byte{1, byte(channel >> 8), byte(channel), 0, 0, 0, byte(len(payload))}
		frame = append(frame, payload...)
		_, err := peer.Write(append(frame, 206))
		return err
	}
	if err := send(0, []byte{0, 10, 0, 10, 0, 9, 0, 0, 0, 0, 0, 0, 0, 5, 'P', 'L', 'A', 'I', 'N', 0, 0, 0, 5, 'e', 'n', '_', 'U', 'S'}); err != nil {
		return err
	}
	if err := read(); err != nil {
		return err
	}
	if err := send(0, []byte{0, 10, 0, 30, 0, 0, 0, 2, 0, 0, 0, 0}); err != nil {
		return err
	}
	for i := 0; i < 2; i++ {
		if err := read(); err != nil {
			return err
		}
	}
	if err := send(0, []byte{0, 10, 0, 41, 0}); err != nil {
		return err
	}
	if err := read(); err != nil {
		return err
	}
	if err := send(1, []byte{0, 20, 0, 11, 0, 0, 0, 0}); err != nil {
		return err
	}
	if err := read(); err != nil {
		return err
	}
	return send(1, []byte{0, 85, 0, 11})
}

func TestWaitWorkersBounded(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Done()
	if waitWorkers(&wg, time.Millisecond) {
		t.Fatal("unfinished worker reported stopped")
	}
}

func TestSessionExitCancelsWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client, peer := net.Pipe()
	defer peer.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); <-ctx.Done() }()
	if !stopWorkers(cancel, client, &wg, time.Second) {
		t.Fatal("session waited before cancelling work")
	}
	if _, err := peer.Write([]byte("x")); err == nil {
		t.Fatal("session left its transport open")
	}
}

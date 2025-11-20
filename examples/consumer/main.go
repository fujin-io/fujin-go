package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"time"

	v1 "github.com/fujin-io/fujin-go/fujin/v1"
)

type TestMsg struct {
	Field string `json:"field"`
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	defer fmt.Println("disconnected")

	conn, err := v1.Dial(ctx, "localhost:4848", &tls.Config{InsecureSkipVerify: true}, nil,
		slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			AddSource: true,
			Level:     slog.LevelDebug,
		})),
		v1.WithTimeout(100*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("client connected")

	defer conn.Close()

	s, err := conn.Init(nil)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("stream connected")

	defer s.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			resp, err := s.HFetch("sub", true, 1)
			if err != nil {
				log.Fatal(err)
			}
			if resp.Error != nil {
				log.Fatal(resp.Error)
			}
			for _, msg := range resp.Messages {
				fmt.Println("Payload:", string(msg.Payload), "Headers:", msg.Headers)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

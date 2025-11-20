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
	"github.com/fujin-io/fujin-go/models"
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

	defer fmt.Println("stream closed")
	defer s.Close()

	sub, err := s.HSubscribe("sub", true, func(msg models.Msg) {
		fmt.Println("Value:", string(msg.Payload), "Headers:", msg.Headers)
	})
	if err != nil {
		log.Fatal(err)
	}
	defer fmt.Println("subscription closed")
	defer s.Unsubscribe(sub)

	sub2, err := s.HSubscribe("sub", true, func(msg models.Msg) {
		fmt.Println("Value:", string(msg.Payload), "Headers:", msg.Headers)
	})
	if err != nil {
		log.Fatal(err)
	}
	defer fmt.Println("subscription 2 closed")
	defer s.Unsubscribe(sub2)

	fmt.Println("subscribed")

	<-ctx.Done()
}

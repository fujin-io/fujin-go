package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
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

	msg := TestMsg{
		Field: "test",
	}

	data, err := json.Marshal(&msg)
	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := s.Produce("pub", data); err != nil {
				log.Fatal(err)
			}
			if err := s.HProduce("pub", data, map[string]string{
				"key": "value",
			}); err != nil {
				log.Fatal(err)
			}
			fmt.Println("message sent")
			time.Sleep(1 * time.Second)
		}
	}
}

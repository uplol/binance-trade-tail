package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/urfave/cli/v2"
)

const (
	BINANCE_STREAM_URL = "wss://stream.binance.com:9443/ws"
)

type Limiter chan struct{}

func NewLimiter() Limiter {
	return make(Limiter, 5)
}

func (l Limiter) Get() {
	l <- struct{}{}
	go func() {
		time.Sleep(time.Second * 1)
		<-l
	}()
}

func run(ctx *cli.Context) error {
	streams := ctx.StringSlice("stream")
	rfc3339 := ctx.Bool("rfc3339-timestamp")

	if ctx.Path("stream-file") != "" {
		file, err := os.OpenFile(ctx.Path("stream-file"), os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		defer file.Close()

		reader := bufio.NewReader(file)
		for {
			line, err := reader.ReadBytes('\n')
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}

			streams = append(streams, strings.TrimSuffix(string(line), "\n"))
		}
	}

	c, _, err := websocket.DefaultDialer.Dial(BINANCE_STREAM_URL, nil)
	if err != nil {
		return err
	}
	defer c.Close()

	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			messageType, message, err := c.ReadMessage()
			if err != nil {
				return
			}

			var data map[string]interface{}
			err = json.Unmarshal(message, &data)
			if err != nil {
				panic(err)
			}

			if _, ok := data["result"]; ok {
				continue
			}

			if messageType != websocket.TextMessage {
				continue
			}

			if rfc3339 {
				data["E"] = time.Unix(0, int64(data["E"].(float64))*int64(time.Millisecond))
				data["T"] = time.Unix(0, int64(data["T"].(float64))*int64(time.Millisecond))

				message, err = json.Marshal(data)
				if err != nil {
					panic(err)
				}
			}

			os.Stdout.Write(append(message, '\r', '\n'))
		}
	}()

	go func() {
		limiter := NewLimiter()
		ticker := time.NewTicker(time.Second * 60)

		for i := 0; i < len(streams); i += 32 {
			end := i + 32

			if end > len(streams) {
				end = len(streams)
			}

			limiter.Get()
			c.WriteJSON(map[string]interface{}{
				"method": "SUBSCRIBE",
				"params": streams[i:end],
				"id":     1,
			})
		}

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				limiter.Get()
				c.WriteMessage(websocket.PingMessage, nil)
			}
		}
	}()

	<-done
	return nil
}

func main() {
	app := &cli.App{
		Name:   "binance-trade-tail",
		Usage:  "tail JSON crypto trade data from the binance streaming api",
		Action: run,
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:  "stream",
				Usage: "provide the name of a stream to subscribe too",
			},
			&cli.PathFlag{
				Name:  "stream-file",
				Usage: "a newline-delimitated file of streams that will be subscribed too",
			},
			&cli.BoolFlag{
				Name:  "rfc3339-timestamp",
				Usage: "rewrite timestamps in RFC 3339 format",
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}
}

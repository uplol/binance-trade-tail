package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"github.com/urfave/cli"
)

const (
	BINANCE_STREAM_URL = "wss://stream.binance.com:9443/ws"
)

func run(ctx *cli.Context) error {
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

			if messageType == websocket.TextMessage {
				os.Stdout.Write(append(message, '\r', '\n'))
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(time.Second * 60)

		c.WriteJSON(map[string]interface{}{
			"method": "SUBSCRIBE",
			"params": ctx.StringSlice("stream"),
			"id":     1,
		})

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
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
				Usage: "a stream to subscribe too",
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}
}

package ws

import (
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"

	"github.com/geometry-labs/icon-logs/config"
	"github.com/geometry-labs/icon-logs/redis"
)

func LogsAddHandlers(app *fiber.App) {

	prefix := config.Config.WebsocketPrefix + "/logs"

	app.Use(prefix, func(c *fiber.Ctx) error {
		// IsWebSocketUpgrade returns true if the client
		// requested upgrade to the WebSocket protocol.
		if websocket.IsWebSocketUpgrade(c) {
			c.Locals("allowed", true)
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})

	app.Get(prefix+"/", websocket.New(handlerGetLogs))
}

func handlerGetLogs(c *websocket.Conn) {

	// Add broadcaster
	msgChan := make(chan []byte)
	id := redis.GetBroadcaster().AddBroadcastChannel(msgChan)
	defer func() {
		// Remove broadcaster
		redis.GetBroadcaster().RemoveBroadcastChannel(id)
	}()

	// Read for close
	clientCloseSig := make(chan bool)
	go func() {
		for {
			_, _, err := c.ReadMessage()
			if err != nil {
				clientCloseSig <- true
				break
			}
		}
	}()

	for {
		// Read
		msg := <-msgChan

		// Broadcast
		err := c.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			break
		}

		// check for client close
		select {
		case _ = <-clientCloseSig:
			break
		default:
			continue
		}
	}
}

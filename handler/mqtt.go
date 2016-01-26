package handler

import (
	"runtime/debug"
	"time"

	"github.com/pquerna/ffjson/ffjson"
	"github.com/shelmesky/message_service/lib"
	"github.com/shelmesky/message_service/utils"
	"github.com/yosssi/gmq/mqtt"
	"github.com/yosssi/gmq/mqtt/client"
)

/*
Connect to MQTT server
*/
func ConnectToMQTTServer(MQTTServerAddress string) (*client.Client, error) {
	defer func() {
		if err := recover(); err != nil {
			utils.Log.Println(err)
			debug.PrintStack()
		}
	}()

	cli := client.New(&client.Options{
		ErrorHandler: func(err error) {
			utils.Log.Println("MQTT Client error:", err)
		},
	})

	var err error

	RandomID := utils.MakeRandomID()

	err = cli.Connect(&client.ConnectOptions{
		Network:         "tcp",
		Address:         MQTTServerAddress,
		ClientID:        []byte(RandomID),
		CleanSession:    true,
		PINGRESPTimeout: 5 * time.Second,
		KeepAlive:       5,
	})

	if err != nil {
		return nil, err
	}

	return cli, nil
}

/*
Forward message to MQTT server
*/
func StartMQTTSender(MQTTServerAddress string, MQTTSendChan chan *lib.PostMessage, channel_name string) chan bool {
	close_chan := make(chan bool)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				utils.Log.Println(err)
				debug.PrintStack()
			}
		}()

		var post_message *lib.PostMessage
		var cli *client.Client
		var err error

		cli, err = ConnectToMQTTServer(MQTTServerAddress)
		if err != nil {
			utils.Log.Println("Connect to MQTT Server failed:", err)
			// if connect failed, wait for close signal
			<-close_chan
			close_chan <- true
			return
		}

		defer cli.Disconnect()

		for {
			select {
			case <-close_chan:
				close_chan <- true
				utils.Log.Printf("Quit MQTT Sender worker.")
				return

			case post_message = <-MQTTSendChan:
				message_json_buffer, err := ffjson.Marshal(post_message)
				if err != nil {
					utils.Log.Println("Marshal JSON failed:", err)
					continue
				}

				err = cli.Publish(&client.PublishOptions{
					QoS:       mqtt.QoS0,
					TopicName: []byte(channel_name),
					Message:   []byte(message_json_buffer),
					Retain:    false,
				})

				if err != nil {
					utils.Log.Println("Publish to MQTT Failed:", err)
					utils.Log.Println("Try to connect MQTT Server.")
					cli, err = ConnectToMQTTServer(MQTTServerAddress)
					if err != nil {
						utils.Log.Println("Connect to MQTT Server failed:", err)
					}
				}

				utils.Log.Printf("Send message to MQTT Server: %s, Length: %d\n", MQTTServerAddress, len(message_json_buffer))

				ffjson.Pool(message_json_buffer)
			}
		}
	}()

	return close_chan
}

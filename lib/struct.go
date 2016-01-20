package lib

import (
	"github.com/shelmesky/message_service/sync"
)

type PollMessage struct {
	Result        int            `json:"result"`
	MessageLength int            `json:"length"`
	MessageList   []*PostMessage `json:"message_list"`
}

type PostMessage struct {
	MessageType string `json:"type"`
	MessageID   string `json:"id"`
	ToUser      string `json:"to_user"`
	Delay       int    `json:"delay"`
	PayLoad     string `json:"payload"`
	Count       uint64
	Lock        *sync.SpinLock
}

type PostReply struct {
	Result    int    `json:"result"`
	MessageID string `json:"id"`
}

type DeleteMessageReply struct {
	Result int `json:"result"`
}

type GeneralOnlineUsers struct {
	Result    int                            `json:"result"`
	Timestamp int64                          `json:"timestamp"`
	UserTags  map[string]*OnlineUsersWithTag `json:"user_tags"`
}

type OnlineUsersWithTag struct {
	Length   int      `json:"length"`
	UserList []string `json:"users"`
}

type OnlineUsers struct {
	Result   int      `json:"result"`
	Length   int      `json:"length"`
	UserList []string `json:"users"`
}

type GeneralOnlineUsersSimple struct {
	Result    int                                  `json:"result"`
	Timestamp int64                                `json:"timestamp"`
	UserTags  map[string]*OnlineUsersSimpleWithTag `json:"user_tags"`
}

type OnlineUsersSimpleWithTag struct {
	Length uint64 `json:"length"`
}

type OnlineUsersSimple struct {
	Result int    `json:"result"`
	Length uint64 `json:"length"`
}

type ChannelStatus struct {
	Name          string `json:"name"`
	UserCount     uint64 `json:"user_count"`
	RealUserCount uint64 `json:"real_user_count"`
}

type ChannelStatusReply struct {
	Result int             `json:"result"`
	Data   []ChannelStatus `json:"data"`
}

type MQTTSingleTopicUsers struct {
	Name   string
	Length uint64
}

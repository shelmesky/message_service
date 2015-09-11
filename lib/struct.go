package lib

type PollMessage struct {
	Result        int            `json:"result"`
	MessageLength int            `json:"length"`
	MessageList   []*PostMessage `json:"message_list"`
}

type PostMessage struct {
	MessageType string `json:"type"`
	MessageID   string `json:"id"`
	ToUser      string `json:"to_user"`
	PayLoad     string `json:"payload"`
}

type PostReply struct {
	Result    int    `json:"result"`
	MessageID string `json:"id"`
}

type DeleteMessageReply struct {
	Result int `json:"result"`
}

type OnlineUsers struct {
	Result   int      `json:"result"`
	Length   int      `json:"length"`
	UserList []string `json:"users"`
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

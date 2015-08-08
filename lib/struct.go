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

package handler

import (
	"container/list"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/shelmesky/bytepool"
	isync "github.com/shelmesky/message_service/sync"
	"github.com/shelmesky/message_service/utils"
	"net/http"
	"sync"
	"time"
)

const (
	CHANNEL_LOCKS             = 8
	CHANNEL_SCAVENGER         = 8
	MULTI_CAST_BUFFER_SIZE    = 1 << 19
	MESSAGE_LIST_SIZE         = 50
	DELAY_CLEAN_USER_RESOURCE = 3600
)

var (
	all_channel *AllChannel

	// object pool
	post_message_pool        *sync.Pool
	post_reply_pool          *sync.Pool
	poll_message_pool        *sync.Pool
	user_pool                *sync.Pool
	user_spinlock_pool       *sync.Pool
	user_message_buffer_pool *sync.Pool

	// byte pool: 4K []byte each of which can hold 8K of data
	byte_pool = bytepool.New(4096, 8192)

	// timewheel
	wheel = utils.NewTimingWheel(1*time.Second, 60)

	ServerDebug bool
)

type AllChannel struct {
	RLock    *sync.RWMutex
	Lock     *sync.Mutex
	Channels map[string]*Channel
}

type Channel struct {
	Name          string
	Users         map[string]*User
	UsersLock     []*sync.RWMutex
	ScavengerChan []chan *User
	MultiCastChan chan *PostMessage
	Count         int64
	//SingleCastChan chan *PostMessage
}

type User struct {
	ID            string
	RemoteAddr    string
	LastUpdate    int64
	SpinLock      *isync.SpinLock
	MessageBuffer *list.List
}

type PollMessage struct {
	Result        int            `json:"result"`
	MessageLength int            `json:"length"`
	MessageList   []*PostMessage `json:"message_list"`
}

type PostMessage struct {
	MessageType string      `json:"type"`
	MessageID   string      `json:"id"`
	ToUser      string      `json:"to_user"`
	PayLoad     interface{} `json:"payload"`
}

type PostReply struct {
	Result    int    `json:"result"`
	MessageID string `json:"id"`
}

type AddChannelReply struct {
	Result int    `json:"result"`
	Data   string `json:"data"`
}

func init() {
	all_channel = new(AllChannel)
	all_channel.RLock = new(sync.RWMutex)
	all_channel.Lock = new(sync.Mutex)
	all_channel.Channels = make(map[string]*Channel, 0)

	post_message_pool = &sync.Pool{
		New: func() interface{} {
			return new(PostMessage)
		},
	}

	post_reply_pool = &sync.Pool{
		New: func() interface{} {
			return new(PostReply)
		},
	}

	poll_message_pool = &sync.Pool{
		New: func() interface{} {
			return new(PollMessage)
		},
	}

	user_pool = &sync.Pool{
		New: func() interface{} {
			return new(User)
		},
	}
}

func NewUser(user_id string) *User {
	user := user_pool.Get().(*User)
	user.ID = user_id
	user.SpinLock = new(isync.SpinLock)
	user.MessageBuffer = list.New()
	user.LastUpdate = time.Now().Unix()
	return user
}

func (this *User) Update() {
}

func (this *User) PushMessage(post_message *PostMessage) {
}

func (this *User) GetMessage() {
}

// 从all_channel中获取Channel，没有则创建
// @channel_name: channel的名称
func GetChannel(channel_name string) *Channel {
	var channel *Channel
	var ok bool

	all_channel.RLock.RLock()
	defer all_channel.RLock.RUnlock()

	if channel, ok = all_channel.Channels[channel_name]; ok {
		return channel
	}

	channel = AddChannel(channel_name)

	return channel
}

func AddChannel(channel_name string) *Channel {
	var lock *sync.RWMutex
	var channel *Channel
	var ok bool

	all_channel.Lock.Lock()
	defer all_channel.Lock.Unlock()

	if channel, ok = all_channel.Channels[channel_name]; !ok {
		utils.Log.Println("Add channel:", channel_name)
		channel = new(Channel)

		// 为每个Channel创建CHANNEL_LOCKS个锁
		// 在Channel中查找用户时，根据user_id和CHANNEL_LOCKS取模获得锁
		for i := 0; i < CHANNEL_LOCKS; i++ {
			lock = new(sync.RWMutex)
			channel.UsersLock = append(channel.UsersLock, lock)
		}
		channel.MultiCastChan = make(chan *PostMessage, MULTI_CAST_BUFFER_SIZE)
		channel.Users = make(map[string]*User, 0)
		channel.Name = channel_name
		channel.Count = 0

		go ChannelSender(channel_name, channel.MultiCastChan)

		// 为每个Channel创建CHANNEL_SCAVENGER个清道夫
		// 定时清除Channel内过期的用户资源
		for j := 0; j < CHANNEL_SCAVENGER; j++ {
			scavenger_chan := make(chan *User, 1024)
			channel.ScavengerChan = append(channel.ScavengerChan, scavenger_chan)
			go ChannelScavenger(channel, scavenger_chan)
		}

		all_channel.Channels[channel_name] = channel
		return channel
	}

	return channel
}

func (this *Channel) getLock(user_id string) (*sync.RWMutex, uint32) {
	user_id_hash := utils.GenKey(user_id)
	user_lock_id := user_id_hash % CHANNEL_LOCKS

	return this.UsersLock[user_lock_id], user_lock_id
}

func (this *Channel) GetUser(user_id string) (*User, error) {
	var user *User
	var ok bool

	users_lock, _ := this.getLock(user_id)

	users_lock.RLock()
	defer users_lock.RUnlock()

	if user, ok = this.Users[user_id]; ok {
		return user, nil
	}

	return user, fmt.Errorf("can not find user [%s : %s]", this.Name, user_id)
}

func (this *Channel) AddUser(user_id string) (*User, error) {
	var user *User
	var ok bool

	users_lock, hash_key := this.getLock(user_id)
	users_lock.Lock()

	if user, ok = this.Users[user_id]; ok {
		users_lock.Unlock()
		return user, fmt.Errorf("user has already exists: [%s : %s]", this.Name, user_id)
	} else {
		user = NewUser(user_id)
		this.Users[user_id] = user
		// 发送用户到清道夫
		this.ScavengerChan[hash_key] <- user
		users_lock.Unlock()
		return user, nil
	}

	users_lock.Unlock()

	return user, fmt.Errorf("can not add user: [%s : %s]", this.Name, user_id)
}

func (this *Channel) DeleteUser(user_id string) (bool, error) {
	users_lock, _ := this.getLock(user_id)
	users_lock.Lock()

	if _, ok := this.Users[user_id]; ok {
		delete(this.Users, user_id)
		users_lock.Unlock()
		return true, nil
	} else {
		users_lock.Unlock()
		return false, fmt.Errorf("can not delete user, it's not exists: [%s : %s]", this.Name, user_id)
	}

	users_lock.Unlock()

	return false, fmt.Errorf("delete user failed: [%s : %s]", this.Name, user_id)
}

func GlobalOptionsHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "channel, tourid")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
}

// 处理创建Channel的请求
func ChannelAddHandler(w http.ResponseWriter, req *http.Request) {
	var add_channel_reply AddChannelReply

	vars := mux.Vars(req)
	channel_name := vars["channel_name"]
	channel := AddChannel(channel_name)

	if channel != nil {
		add_channel_reply.Result = 0
		add_channel_reply.Data = "create channel successful"
	} else {
		add_channel_reply.Result = 1
		add_channel_reply.Data = "channel already exists"
	}

	buf, err := json.Marshal(add_channel_reply)
	if err != nil {
		utils.Log.Printf("[%s] Marshal JSON failed: [%s], channel: [%s]\n", req.RemoteAddr, err, channel_name)
		http.Error(w, "Marshal JSON failed", 500)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(buf)
}

// 处理POST消息
func MessagePostHandler(w http.ResponseWriter, req *http.Request) {
	var channel_name string
	var channel *Channel
	var err error
	var buf []byte

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "channel, tourid")
	w.Header().Set("Access-Control-Allow-Credentials", "true")

	channel_name = req.Header.Get("channel")
	if channel_name == "" {
		utils.Log.Printf("[%s] channel name not in header\n", req.RemoteAddr)
		http.Error(w, "channel name not in header", 400)
		return
	}

	// use byte pool
	buffer := byte_pool.Checkout()
	defer buffer.Close()
	buffer.ReadFrom(req.Body)
	body := buffer.Bytes()

	post_message := post_message_pool.Get().(*PostMessage)
	err = json.Unmarshal(body, post_message)
	if err != nil {
		utils.Log.Printf("[%s] Unmarshal json failed: [%s], channel: [%s]\n", req.RemoteAddr, err, channel_name)
		http.Error(w, "Unmarshal json failed", 500)
		return
	}

	channel = GetChannel(channel_name)

	message_id := utils.MakeRandomID()
	post_message.MessageID = message_id

	send_finished := false
	// send message to buffered channel
	select {
	case channel.MultiCastChan <- post_message:
		send_finished = true
	case _ = <-wheel.After(20 * time.Millisecond):
		utils.Log.Println("message buffer of channel is full, channel:", channel_name)
		send_finished = false
	}

	post_reply := post_reply_pool.Get().(*PostReply)
	if send_finished {
		post_reply.Result = 0
		post_reply.MessageID = message_id
	} else {
		post_reply.Result = 1
		post_reply.MessageID = "message buffer of channel is full."
	}

	buf, err = json.Marshal(*post_reply)
	if err != nil {
		utils.Log.Printf("[%s] Marshal JSON failed: [%s], channel: [%s]\n", req.RemoteAddr, err, channel_name)
		http.Error(w, "Marshal json failed", 500)
		return
	}

	if ServerDebug {
		utils.Log.Printf("Got message from [%s], message: [%s], message_id: [%s], channel: [%s]\n", req.RemoteAddr, string(body), message_id, channel_name)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(buf)

	post_reply_pool.Put(post_reply)
}

// 处理Poll消息
func MessagePollHandler(w http.ResponseWriter, req *http.Request) {
	var channel_name string
	var user_id string

	var message_list []*PostMessage
	var message_list_raw []*list.Element

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "channel, tourid")
	w.Header().Set("Access-Control-Allow-Credentials", "true")

	channel_name = req.Header.Get("channel")
	if channel_name == "" {
		utils.Log.Printf("[%s] channel name not in header\n", req.RemoteAddr)
		http.Error(w, "channel name not in header", 400)
		return
	}

	user_id = req.Header.Get("tourid")
	if user_id == "" {
		utils.Log.Printf("[%s] user_id not in header\n", req.RemoteAddr)
		http.Error(w, "user_id name not in header", 400)
		return
	}

	channel := GetChannel(channel_name)

	user, err := channel.GetUser(user_id)
	if err != nil {
		user, err = channel.AddUser(user_id)
		if err != nil {
			utils.Log.Printf("[%s] AddUser failed: [%s]\n", req.RemoteAddr, err)
		}
	}

	message_list_size := 0
	user.SpinLock.Lock()
	if user.MessageBuffer != nil {
		for e := user.MessageBuffer.Front(); e != nil; e = e.Next() {
			if message_list_size == MESSAGE_LIST_SIZE {
				break
			}
			if post_message, ok := e.Value.(*PostMessage); ok {
				message_list = append(message_list, post_message)
				message_list_raw = append(message_list_raw, e)
				message_list_size += 1
			}
		}

		for idx := range message_list_raw {
			element := message_list_raw[idx]
			user.MessageBuffer.Remove(element)
		}
	}

	user.LastUpdate = time.Now().Unix()
	user.SpinLock.Unlock()

	poll_message := poll_message_pool.Get().(*PollMessage)
	poll_message.Result = 0
	poll_message.MessageLength = len(message_list)
	if len(message_list) == 0 {
		poll_message.MessageList = []*PostMessage{}
	} else {
		poll_message.MessageList = message_list
	}

	buf, err := json.Marshal(*poll_message)
	if err != nil {
		utils.Log.Printf("[%s] Marshal JSON failed: [%s], channel: [%s]\n", req.RemoteAddr, err, channel_name)
		http.Error(w, "Marshal json failed", 500)
		return
	}

	if ServerDebug == true {
		utils.Log.Printf("Send message to [%s], message: [%s], channel: [%s], user_id: [%s]\n", req.RemoteAddr, string(buf), channel_name, user_id)
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(buf)

	for idx := range message_list {
		post_message_pool.Put(message_list[idx])
	}

	poll_message_pool.Put(poll_message)
}

func ChannelSender(channel_name string, multicast_channel chan *PostMessage) {
	for {
		channel := GetChannel(channel_name)

		// 如果channel中有用户，或正确获取channel
		// 则保存消息到用户的消息缓存
		if len(channel.Users) > 0 {
			post_message := <-multicast_channel

			userid := post_message.ToUser

			if userid == "" {
				for key := range channel.Users {
					if user, ok := channel.Users[key]; ok {
						user.SpinLock.Lock()
						user.MessageBuffer.PushBack(post_message)
						user.SpinLock.Unlock()
					}
				}
			} else {
				if user, ok := channel.Users[userid]; ok {
					user.SpinLock.Lock()
					user.MessageBuffer.PushBack(post_message)
					user.SpinLock.Unlock()
				}
			}
		} else {
			// channel中不存在用户，或获取channel失败
			// 暂停500毫秒
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// 定时清除用户和相关资源
func ChannelScavenger(channel *Channel, scavenger_chan chan *User) {
	var user *User

	user_list := make(map[string]*User, 1024)

	time.Sleep(5 * time.Second)

	for {
		select {
		case user := <-scavenger_chan:
			utils.Log.Println("Scavenger receive user:", user.ID)
			user_list[user.ID] = user
		case _ = <-wheel.After(2 * time.Second):
			if len(user_list) > 0 {
				for idx := range user_list {
					now := time.Now().Unix()
					user = user_list[idx]
					if now-user.LastUpdate > DELAY_CLEAN_USER_RESOURCE {
						user.SpinLock.Lock()
						user.MessageBuffer.Init()
						delete(channel.Users, user.ID)
						delete(user_list, user.ID)
						user.SpinLock.Unlock()
						utils.Log.Println("Scavenger clean user:", user.ID)
						user_pool.Put(user)
					}
				}
			}
		}
	}
}

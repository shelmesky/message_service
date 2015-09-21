package handler

import (
	"container/list"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/pquerna/ffjson/ffjson"
	"github.com/shelmesky/bytepool"
	"github.com/shelmesky/message_service/lib"
	isync "github.com/shelmesky/message_service/sync"
	"github.com/shelmesky/message_service/utils"
	"io/ioutil"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	CHANNEL_LOCKS     = 4
	CHANNEL_SCAVENGER = 4

	/* we support multiple channel,
	so we must use small buffer size of channel,
	except you use single channel for all user */
	MULTI_CAST_BUFFER_SIZE         = 1 << 10
	MULTI_CAST_STAGE_0_BUFFER_SIZE = 1 << 10

	DELAY_CLEAN_USER_RESOURCE    = 30
	DELAY_USER_ONLINE            = 120
	DELAY_CLEAN_CHANNEL_RESOURCE = 1
	DELAY_CHANNEL_POST           = 60

	USE_FASE_ONLINE_MAP = true

	CHANGE_USER_STATE_IN_REAL_TIME = true

	//MULTI_CAST_BUFFER_SIZE	= 1 << 19
	//CHANNEL_LOCKS				= 8
	//CHANNEL_SCAVENGER			= 8
)

var (
	all_channel *AllChannel

	// byte pool: 4K []byte each of which can hold 8K of data
	byte_pool = bytepool.New(4096, 8192)

	// timingwheel
	wheel_seconds      = utils.NewTimingWheel(1*time.Second, 10)
	wheel_milliseconds = utils.NewTimingWheel(10*time.Millisecond, 2)

	ServerDebug bool

	Config *lib.GlobalConfig
)

type AllChannel struct {
	RLock    *sync.RWMutex
	Lock     *sync.Mutex
	Channels map[string]*Channel
}

type UserState struct {
	ID    string
	Tag   string
	State bool
	From  int
}

type Channel struct {
	Name            string
	Users           map[string]*User
	UsersLock       []*sync.RWMutex
	OnlineUsers     map[string]*UserState
	OnlineUsersLock *sync.RWMutex
	UserCount       uint64
	RealUserCount   uint64

	ScavengerChan       []chan *User
	UserChan            []chan *User
	MultiCastStage0Chan chan *lib.PostMessage
	MultiCastStage1Chan chan *lib.PostMessage
	MultiCastStage2Chan []chan *lib.PostMessage
	UserStateChan       chan *UserState

	PostMessagePool *sync.Pool
	PostReplyPool   *sync.Pool
	PollMessagePool *sync.Pool
	UserStatePool   *sync.Pool

	ChannelRLock *sync.RWMutex

	PrepareClose bool
	Closed       bool
	CloseChan    []chan bool

	LastPostUpdate int64

	//SingleCastChan chan *PostMessage
}

type User struct {
	ID            string
	RemoteAddr    string
	LastUpdate    int64
	SpinLock      *isync.SpinLock
	MessageBuffer *list.List
	Online        bool
	SenderKey     uint32
	Tag           string
}

type AddChannelReply struct {
	Result int    `json:"result"`
	Data   string `json:"data"`
}

// 修改全局配置的请求
type ConfigAction struct {
	ActionType string `json:"action_type"`
	Operation  string `json:"operation"`
}

type ConfigActionReply struct {
	Result int    `json:"result"`
	Data   string `json:"data"`
}

func init() {
	all_channel = new(AllChannel)
	all_channel.RLock = new(sync.RWMutex)
	all_channel.Lock = new(sync.Mutex)
	all_channel.Channels = make(map[string]*Channel, 0)

	StartGlobalScavenger()
}

func NewUser(user_id string) *User {
	user := new(User)
	user.ID = user_id
	user.SpinLock = new(isync.SpinLock)
	user.MessageBuffer = list.New()
	user.LastUpdate = time.Now().Unix()
	return user
}

func (this *User) Update() {
}

func (this *User) PushMessage(post_message *lib.PostMessage) {
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

		// 传递用户状态的channel
		channel.UserStateChan = make(chan *UserState, 1024)

		// 多级channel
		channel.MultiCastStage0Chan = make(chan *lib.PostMessage, MULTI_CAST_STAGE_0_BUFFER_SIZE)
		channel.MultiCastStage1Chan = make(chan *lib.PostMessage, MULTI_CAST_BUFFER_SIZE)

		channel.Users = make(map[string]*User, 0)
		channel.Name = channel_name
		channel.UserCount = 0
		channel.ChannelRLock = new(sync.RWMutex)

		// 保存channel的在线用户
		channel.OnlineUsers = make(map[string]*UserState, 1024)
		channel.OnlineUsersLock = new(sync.RWMutex)

		// 创建Stage0/1/2的Sender
		for j := 0; j < CHANNEL_LOCKS; j++ {
			user_chan := make(chan *User, 1024)
			channel.UserChan = append(channel.UserChan, user_chan)
			stage_channel := make(chan *lib.PostMessage, MULTI_CAST_BUFFER_SIZE)
			channel.MultiCastStage2Chan = append(channel.MultiCastStage2Chan, stage_channel)
			close_chan := StartChannelSenderStage2(channel_name, user_chan, stage_channel, j)
			channel.CloseChan = append(channel.CloseChan, close_chan)
		}

		close_chan := StartChannelSenderStage0(channel_name, channel.MultiCastStage0Chan, channel.MultiCastStage1Chan)
		channel.CloseChan = append(channel.CloseChan, close_chan)
		close_chan = StartChannelSenderStage1(channel_name, channel.MultiCastStage1Chan, channel.MultiCastStage2Chan)
		channel.CloseChan = append(channel.CloseChan, close_chan)

		// 维护在线用户列表
		close_chan = StartUserStateCollector(channel)
		channel.CloseChan = append(channel.CloseChan, close_chan)

		// 为每个Channel创建CHANNEL_SCAVENGER个清道夫
		// 定时清除Channel内过期的用户资源
		for k := 0; k < CHANNEL_SCAVENGER; k++ {
			scavenger_chan := make(chan *User, 1024)
			channel.ScavengerChan = append(channel.ScavengerChan, scavenger_chan)
			close_chan = StartChannelScavenger(channel, scavenger_chan, k, channel.UserStateChan)
			channel.CloseChan = append(channel.CloseChan, close_chan)
		}

		channel.LastPostUpdate = time.Now().Unix()

		// 对象池
		channel.PostMessagePool = &sync.Pool{
			New: func() interface{} {
				return new(lib.PostMessage)
			},
		}

		channel.PostReplyPool = &sync.Pool{
			New: func() interface{} {
				return new(lib.PostReply)
			},
		}

		channel.PollMessagePool = &sync.Pool{
			New: func() interface{} {
				return new(lib.PollMessage)
			},
		}

		channel.UserStatePool = &sync.Pool{
			New: func() interface{} {
				return new(UserState)
			},
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
		// 保存用户的SenderKey
		user.SenderKey = hash_key
		// 发送用户到清道夫和Stage2 Sender
		this.ScavengerChan[hash_key] <- user
		this.UserChan[hash_key] <- user
		users_lock.Unlock()
		return user, nil
	}

	users_lock.Unlock()

	return user, fmt.Errorf("can not add user: [%s : %s]", this.Name, user_id)
}

func (this *Channel) DeleteUser(user_id string) (bool, error) {
	users_lock, _ := this.getLock(user_id)
	users_lock.Lock()

	if user, ok := this.Users[user_id]; ok {
		user.MessageBuffer.Init()
		user.MessageBuffer = nil
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

// 处理实时修改配置的请求
func SysConfigHandler(w http.ResponseWriter, req *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			utils.Log.Println(err)
			debug.PrintStack()
		}
	}()

	var config_action ConfigAction
	var config_action_reply ConfigActionReply

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "channel, tourid")
	w.Header().Set("Access-Control-Allow-Credentials", "true")

	buf, err := ioutil.ReadAll(req.Body)
	if err != nil {
		utils.Log.Printf("Read data from: [%s] failed.\n", req.RemoteAddr)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	err = json.Unmarshal(buf, &config_action)
	if err != nil {
		utils.Log.Printf("[%s] Unmarshal json failed.\n", req.RemoteAddr)
		http.Error(w, "Unmarshal json failed.", 500)
		return
	}

	if config_action.ActionType != "set_gc" && config_action.ActionType != "set_gc_free_memory" {
		config_action_reply.Result = 1
		config_action_reply.Data = "bad action type"
		goto end
	}

	Config.Lock.Lock()
	defer Config.Lock.Unlock()

	if config_action.ActionType == "set_gc" {
		if config_action.Operation == "enable" {
			if Config.ForceGC == true {
				config_action_reply.Result = 1
				config_action_reply.Data = "ForceGC is already enabled"
			} else {
				Config.ForceGC = true
				config_action_reply.Result = 0
				config_action_reply.Data = "Enable ForceGC success"
			}
		}

		if config_action.Operation == "disable" {
			if Config.ForceGC == false {
				config_action_reply.Result = 1
				config_action_reply.Data = "ForceGC is already disabled"
			} else {
				Config.ForceGC = false
				config_action_reply.Result = 0
				config_action_reply.Data = "Disable ForceGC success"
			}
		}
	}

	if config_action.ActionType == "set_gc_free_memory" {
		if config_action.Operation == "enable" {
			if Config.ForceFreeOSMemory == true {
				config_action_reply.Result = 1
				config_action_reply.Data = "ForceFreeOSMemory is already enabled"
			} else {
				config_action_reply.Result = 0
				config_action_reply.Data = "Enable ForceFreeOSMemory success"
				Config.ForceFreeOSMemory = true
			}
		}

		if config_action.Operation == "disable" {
			if Config.ForceFreeOSMemory == false {
				config_action_reply.Result = 1
				config_action_reply.Data = "ForceFreeOSMemory is already disabled"
			} else {
				config_action_reply.Result = 0
				config_action_reply.Data = "Disable ForceFreeOSMemory success"
				Config.ForceFreeOSMemory = false
			}
		}
	}

end:
	buf, err = json.Marshal(config_action_reply)
	if err != nil {
		utils.Log.Printf("[%s] Marshal json failed.\n", req.RemoteAddr)
		http.Error(w, "Marshal json failed.", 500)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(buf)
}

func SysStatusHandler(w http.ResponseWriter, req *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			utils.Log.Println(err)
			debug.PrintStack()
		}
	}()

	var key string
	var channel *Channel
	var channel_status lib.ChannelStatus
	var channel_status_reply lib.ChannelStatusReply

	all_channel.RLock.RLock()
	defer all_channel.RLock.RUnlock()

	for key = range all_channel.Channels {
		channel = all_channel.Channels[key]
		channel_status.Name = channel.Name
		channel_status.UserCount = atomic.LoadUint64(&channel.UserCount)
		channel_status.RealUserCount = atomic.LoadUint64(&channel.RealUserCount)
		channel_status_reply.Data = append(channel_status_reply.Data, channel_status)
	}

	if len(channel_status_reply.Data) == 0 {
		channel_status_reply.Result = 0
		channel_status_reply.Data = []lib.ChannelStatus{}
	}

	buf, err := ffjson.Marshal(channel_status_reply)
	if err != nil {
		utils.Log.Printf("[%s] Marshal JSON failed: [%s]\n", req.RemoteAddr, err)
		http.Error(w, "Marshal json failed", 500)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(buf)

	ffjson.Pool(buf)
}

// 处理创建Channel的请求
func ChannelAddHandler(w http.ResponseWriter, req *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			utils.Log.Println(err)
			debug.PrintStack()
		}
	}()

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
	defer func() {
		if err := recover(); err != nil {
			utils.Log.Println(err)
			debug.PrintStack()
		}
	}()

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

	channel = GetChannel(channel_name)

	post_message := channel.PostMessagePool.Get().(*lib.PostMessage)

	err = ffjson.Unmarshal(body, post_message)
	if err != nil {
		utils.Log.Printf("[%s] Unmarshal json failed: [%s], channel: [%s]\n", req.RemoteAddr, err, channel_name)
		http.Error(w, "Unmarshal json failed", 500)
		return
	}

	if channel.PrepareClose == true {
		utils.Log.Printf("[%s] Channel: [%s] will be closed.\n", req.RemoteAddr, channel_name)
		http.Error(w, "channel will be closed", 500)
		return
	}

	message_id := utils.MakeRandomID()
	post_message.MessageID = message_id

	send_finished := false
	// send message to buffered channel
	select {
	case channel.MultiCastStage0Chan <- post_message:
		send_finished = true
	case _ = <-wheel_milliseconds.After(10 * time.Millisecond):
		utils.Log.Println("message buffer of stage0 channel is full, channel:", channel_name)
		send_finished = false
	}

	post_reply := channel.PostReplyPool.Get().(*lib.PostReply)
	if send_finished {
		post_reply.Result = 0
		post_reply.MessageID = message_id
	} else {
		post_reply.Result = 1
		post_reply.MessageID = "message buffer of channel is full."
	}

	buf, err = ffjson.Marshal(*post_reply)
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

	channel.PostReplyPool.Put(post_reply)
	ffjson.Pool(buf)
}

func OnlineUsersSimpleHandler(w http.ResponseWriter, req *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			utils.Log.Println(err)
			debug.PrintStack()
		}
	}()

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "channel, tourid")

	var online_users_simple lib.OnlineUsersSimple
	var channel_name string

	all_channel.RLock.RLock()
	defer all_channel.RLock.RUnlock()

	channel_name = req.Header.Get("channel")
	channel_name = strings.Trim(channel_name, " ")
	if channel_name == "" {
		utils.Log.Printf("[%s] channel name not in header\n", req.RemoteAddr)
		http.Error(w, "channel name not in header", 400)
		return
	}

	channel := GetChannel(channel_name)

	online_users_simple.Length = atomic.LoadUint64(&channel.RealUserCount)
	online_users_simple.Result = 0

	buf, err := ffjson.Marshal(online_users_simple)
	if err != nil {
		utils.Log.Printf("[%s] Marshal JSON failed: [%s], channel: [%s]\n", req.RemoteAddr, err, channel_name)
		http.Error(w, "Marshal json failed", 500)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(buf)

	ffjson.Pool(buf)
}

func OnlineUsersSimpleHandlerWithTag(w http.ResponseWriter, req *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			utils.Log.Println(err)
			debug.PrintStack()
		}
	}()

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "channel, tourid")

	var channel_name string
	var general_online_users_simple lib.GeneralOnlineUsersSimple
	var user_state_tag_map *lib.OnlineUsersSimpleWithTag
	var ok bool

	temp_tag_map := make(map[string]*lib.OnlineUsersSimpleWithTag, 10)

	channel_name = req.Header.Get("channel")
	if channel_name == "" {
		utils.Log.Printf("[%s] channel name not in header\n", req.RemoteAddr)
		http.Error(w, "channel name not in header", 400)
		return
	}

	channel := GetChannel(channel_name)

	channel.OnlineUsersLock.RLock()
	for username := range channel.OnlineUsers {
		state := channel.OnlineUsers[username]

		if user_state_tag_map, ok = temp_tag_map[state.Tag]; !ok {
			user_state_tag_map = new(lib.OnlineUsersSimpleWithTag)
			temp_tag_map[state.Tag] = user_state_tag_map
		}

		temp_tag_map[state.Tag].Length += 1
	}
	channel.OnlineUsersLock.RUnlock()

	general_online_users_simple.Result = 0
	general_online_users_simple.UserTags = temp_tag_map

	buf, err := ffjson.Marshal(general_online_users_simple)
	if err != nil {
		utils.Log.Printf("[%s] Marshal JSON failed: [%s], channel: [%s]\n", req.RemoteAddr, err, channel_name)
		http.Error(w, "Marshal json failed", 500)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(buf)

	ffjson.Pool(buf)
}

func OnlineUsersHandler(w http.ResponseWriter, req *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			utils.Log.Println(err)
			debug.PrintStack()
		}
	}()

	var online_users lib.OnlineUsers
	var channel_name string
	var user *User
	var key string
	var now int64

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "channel, tourid")

	channel_name = req.Header.Get("channel")
	channel_name = strings.Trim(channel_name, " ")
	if channel_name == "" {
		utils.Log.Printf("[%s] channel name not in header\n", req.RemoteAddr)
		http.Error(w, "channel name not in header", 400)
		return
	}

	channel := GetChannel(channel_name)

	if USE_FASE_ONLINE_MAP == false {
		channel.ChannelRLock.RLock()

		channel_user_length := len(channel.Users)
		if channel_user_length > 0 {

			for key = range channel.Users {
				now = time.Now().Unix()
				user = channel.Users[key]
				if now-user.LastUpdate < DELAY_USER_ONLINE {
					online_users.UserList = append(online_users.UserList, key)
				}
			}

			if len(online_users.UserList) == 0 {
				online_users.UserList = []string{}
			}

		} else {
			online_users.UserList = []string{}
		}

		channel.ChannelRLock.RUnlock()

	} else {
		channel.OnlineUsersLock.RLock()
		for key = range channel.OnlineUsers {
			online_users.UserList = append(online_users.UserList, key)
		}
		channel.OnlineUsersLock.RUnlock()
	}

	online_users.Result = 0
	online_users.Length = len(online_users.UserList)

	if online_users.Length == 0 {
		online_users.UserList = []string{}
	}

	buf, err := ffjson.Marshal(online_users)
	if err != nil {
		utils.Log.Printf("[%s] Marshal JSON failed: [%s], channel: [%s]\n", req.RemoteAddr, err, channel_name)
		http.Error(w, "Marshal json failed", 500)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(buf)

	ffjson.Pool(buf)
}

func OnlineUsersHandlerWithTag(w http.ResponseWriter, req *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			utils.Log.Println(err)
			debug.PrintStack()
		}
	}()

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "channel, tourid")

	var channel_name string
	var general_online_users lib.GeneralOnlineUsers
	var user_state_tag_map *lib.OnlineUsersWithTag
	var ok bool

	temp_tag_map := make(map[string]*lib.OnlineUsersWithTag, 10)

	channel_name = req.Header.Get("channel")
	if channel_name == "" {
		utils.Log.Printf("[%s] channel name not in header\n", req.RemoteAddr)
		http.Error(w, "channel name not in header", 400)
		return
	}

	channel := GetChannel(channel_name)

	channel.OnlineUsersLock.RLock()
	for username := range channel.OnlineUsers {
		state := channel.OnlineUsers[username]

		if user_state_tag_map, ok = temp_tag_map[state.Tag]; !ok {
			user_state_tag_map = new(lib.OnlineUsersWithTag)
			temp_tag_map[state.Tag] = user_state_tag_map
		}

		temp_tag_map[state.Tag].Length += 1
		temp_tag_map[state.Tag].UserList = append(temp_tag_map[state.Tag].UserList, username)
	}
	channel.OnlineUsersLock.RUnlock()

	general_online_users.Result = 0
	general_online_users.UserTags = temp_tag_map

	buf, err := ffjson.Marshal(general_online_users)
	if err != nil {
		utils.Log.Printf("[%s] Marshal JSON failed: [%s], channel: [%s]\n", req.RemoteAddr, err, channel_name)
		http.Error(w, "Marshal json failed", 500)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(buf)

	ffjson.Pool(buf)

}

func MessageDeleteHandler(w http.ResponseWriter, req *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			utils.Log.Println(err)
			debug.PrintStack()
		}
	}()

	var channel_name string
	var user_id string
	var delete_message_reply lib.DeleteMessageReply

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "channel, tourid")

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

	user.SpinLock.Lock()
	if user.MessageBuffer != nil {
		user.MessageBuffer = user.MessageBuffer.Init()
	}
	user.SpinLock.Unlock()

	delete_message_reply.Result = 0

	buf, err := ffjson.Marshal(delete_message_reply)
	if err != nil {
		utils.Log.Printf("[%s] Marshal JSON failed: [%s], channel: [%s]\n", req.RemoteAddr, err, channel_name)
		http.Error(w, "Marshal json failed", 500)
		return
	}

	message_buffer_len := user.MessageBuffer.Len()
	if ServerDebug == true {
		utils.Log.Printf("Delete message for [%s], channel: [%s], user_id: [%s], length: [%d]\n", req.RemoteAddr, channel_name, user_id, message_buffer_len)
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(buf)

	ffjson.Pool(buf)
}

// 处理Poll消息
func MessagePollHandler(w http.ResponseWriter, req *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			utils.Log.Println(err)
			debug.PrintStack()
		}
	}()

	var channel_name string
	var user_id string

	var message_list []*lib.PostMessage

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "channel, tourid")

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

	user_tag := req.Header.Get("tag")
	if user_tag == "" {
		utils.Log.Printf("[%s] user_tag not in header\n", req.RemoteAddr)
	}

	channel := GetChannel(channel_name)

	if channel.PrepareClose == true {
		utils.Log.Printf("[%s] Channel: [%s] will be closed.\n", req.RemoteAddr, channel_name)
		http.Error(w, "channel will be closed", 500)
		return
	}

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
		for i := 0; i < Config.PollMessageSize; i++ {
			if user.MessageBuffer.Len() == 0 {
				break
			}
			e := user.MessageBuffer.Front()
			if e != nil {
				if post_message, ok := user.MessageBuffer.Remove(e).(*lib.PostMessage); ok {
					message_list = append(message_list, post_message)
					message_list_size += 1
				}
			}
		}
	}

	// update user's tag when tag was changed
	if CHANGE_USER_STATE_IN_REAL_TIME && user_tag != "" {
		if user_tag != user.Tag {
			state := channel.UserStatePool.Get().(*UserState)
			state.ID = user.ID
			state.Tag = user.Tag
			state.State = true
			state.From = 1
			channel.UserStateChan <- state
		}

		user.Tag = user_tag
	}

	user.LastUpdate = time.Now().Unix()
	user.SpinLock.Unlock()

	poll_message := channel.PollMessagePool.Get().(*lib.PollMessage)
	poll_message.Result = 0
	poll_message.MessageLength = len(message_list)
	if len(message_list) == 0 {
		poll_message.MessageList = []*lib.PostMessage{}
	} else {
		poll_message.MessageList = message_list
	}

	buf, err := ffjson.Marshal(*poll_message)
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
		channel.PostMessagePool.Put(message_list[idx])
	}

	channel.PollMessagePool.Put(poll_message)
	ffjson.Pool(buf)
}

func StartChannelSenderStage0(channel_name string, stage0_channel, stage1_channel chan *lib.PostMessage) chan bool {
	close_chan := make(chan bool)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				utils.Log.Println(err)
				debug.PrintStack()
			}
		}()

		var post_message *lib.PostMessage

		channel := GetChannel(channel_name)

		for {
			select {
			case <-close_chan:
				utils.Log.Printf("Channel [%s] SenderStage0 has quit...\n", channel_name)
				close_chan <- true
				return

			case post_message = <-stage0_channel:
				stage1_channel <- post_message
				channel.LastPostUpdate = time.Now().Unix()
				if ServerDebug {
					utils.Log.Println("ChannelSenderStage0: send post_message to stage1_channel", post_message)
				}
			}
		}
	}()

	return close_chan
}

func StartChannelSenderStage1(channel_name string, stage1_channel chan *lib.PostMessage, stage2_channel_list []chan *lib.PostMessage) chan bool {
	close_chan := make(chan bool)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				utils.Log.Println(err)
				debug.PrintStack()
			}
		}()

		var idx int
		var post_message *lib.PostMessage

		channel := GetChannel(channel_name)

		for {
			select {
			case <-close_chan:
				utils.Log.Printf("Channel [%s] SenderStage1 has quit...\n", channel_name)
				close_chan <- true
				return

			case post_message = <-stage1_channel:
				if post_message.ToUser == "" {
					// channel内广播消息
					for idx = range stage2_channel_list {
						new_post_message := CopyMessage(channel, post_message)
						select {
						case stage2_channel_list[idx] <- new_post_message:
						case _ = <-wheel_milliseconds.After(10 * time.Millisecond):
							utils.Log.Printf("ChannelSenderStage1: Stage2 channel is full, channel: %s!!!\n", channel_name)
						}
					}
					channel.PostMessagePool.Put(post_message)
				} else {
					// 发送给channel的指定用户
					user_id_hash := utils.GenKey(post_message.ToUser)
					hash_key := user_id_hash % CHANNEL_LOCKS
					select {
					case stage2_channel_list[hash_key] <- post_message:
					case _ = <-wheel_milliseconds.After(10 * time.Millisecond):
						utils.Log.Printf("ChannelSenderStage1: Stage2 channel is full, channel: %s!!!\n", channel_name)
					}
				}
				if ServerDebug {
					utils.Log.Println("ChannelSenderStage1: send post_message to stage2_channel", post_message)
				}
			}
		}
	}()

	return close_chan
}

func StartChannelSenderStage2(channel_name string, user_channel chan *User, stage2_channel chan *lib.PostMessage, idx int) chan bool {
	close_chan := make(chan bool)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				utils.Log.Println(err)
				debug.PrintStack()
			}
		}()

		var post_message *lib.PostMessage
		var user *User
		var ok bool

		user_list := make(map[string]*User, 1024)
		post_message_temp_buffer := make([]*lib.PostMessage, 0)

		// 如果当前Sender存在用户则保存到用户消息缓存
		// 否则保存到当前Sender的消息缓存
		// 等到接收到第一个用户时，发送给用户

		channel := GetChannel(channel_name)

		for {
			select {
			case <-close_chan:
				utils.Log.Printf("Channel [%s] SenderStage2 [%d] has quit...\n", channel_name, idx)
				close_chan <- true
				return

			case user = <-user_channel:
				user_list[user.ID] = user
				utils.Log.Printf("Channel [%s] SenderStage2 [%d] got user: %s\n", channel_name, idx, user.ID)

				// 发送本地缓存中的消息给用户
				if len(post_message_temp_buffer) > 0 {
					for key := range user_list {
						if user, ok = user_list[key]; ok {
							for idx := range post_message_temp_buffer {
								new_post_message := CopyMessage(channel, post_message_temp_buffer[idx])
								user.SpinLock.Lock()
								user.MessageBuffer.PushBack(new_post_message)
								user.SpinLock.Unlock()
							}
						}
					}
					for idx := range post_message_temp_buffer {
						channel.PostMessagePool.Put(post_message_temp_buffer[idx])
					}
					post_message_temp_buffer = nil
					post_message_temp_buffer = make([]*lib.PostMessage, 0)
				}

			case post_message = <-stage2_channel:
				if len(user_list) > 0 {

					userid := post_message.ToUser

					if userid == "" {
						for key := range user_list {
							if user, ok = user_list[key]; ok {
								if user == nil {
									delete(user_list, key)
									utils.Log.Printf("Channel SenderStage2 [%d] clean user: %s\n", idx, key)
									continue
								}

								if user.MessageBuffer == nil {
									delete(user_list, key)
									utils.Log.Printf("Channel SenderStage2 [%d] clean user: %s\n", idx, key)
									continue
								}

								new_post_message := CopyMessage(channel, post_message)
								user.SpinLock.Lock()
								user.MessageBuffer.PushBack(new_post_message)
								user.SpinLock.Unlock()
							}
						}
						channel.PostMessagePool.Put(post_message)
					} else {
						if user, ok := channel.Users[userid]; ok {
							user.SpinLock.Lock()
							user.MessageBuffer.PushBack(post_message)
							user.SpinLock.Unlock()
						}
					}
				} else {
					// TODO: fix me: temp buffer cause GC heavily
					new_post_message := CopyMessage(channel, post_message)
					post_message_temp_buffer = append(post_message_temp_buffer, new_post_message)
					channel.PostMessagePool.Put(post_message)
				}
			}
		}
	}()

	return close_chan
}

func CopyMessage(channel *Channel, post_message *lib.PostMessage) *lib.PostMessage {
	new_post_message := channel.PostMessagePool.Get().(*lib.PostMessage)
	new_post_message.MessageType = post_message.MessageType
	new_post_message.MessageID = post_message.MessageID
	new_post_message.ToUser = post_message.ToUser
	new_post_message.PayLoad = post_message.PayLoad
	return new_post_message
}

// 维护channel的在线用户列表
func StartUserStateCollector(channel *Channel) chan bool {
	close_chan := make(chan bool)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				utils.Log.Println(err)
				debug.PrintStack()
			}
		}()

		var user_state *UserState
		var ok bool

		for {
			select {
			case <-close_chan:
				utils.Log.Printf("Channel [%s] UserStateCollector has quit...\n", channel.Name)
				close_chan <- true
				return

			case user_state = <-channel.UserStateChan:
				if ServerDebug == true {
					utils.Log.Printf("Channel [%s] User change state: [%s: %s]\n", channel.Name, user_state.ID, user_state.State)
				}

				channel.OnlineUsersLock.Lock()
				if user_state.State == true {
					channel.OnlineUsers[user_state.ID] = user_state
					if user_state.From == 0 {
						atomic.AddUint64(&channel.RealUserCount, 1)
					}
				} else {
					if user_state, ok = channel.OnlineUsers[user_state.ID]; ok {
						if user_state.From == 0 {
							delete(channel.OnlineUsers, user_state.ID)
							atomic.AddUint64(&channel.RealUserCount, ^uint64(0))
						}
						channel.UserStatePool.Put(user_state)
					}
				}
				channel.OnlineUsersLock.Unlock()
			}
		}
	}()

	return close_chan
}

// 定时清除用户和相关资源
func StartChannelScavenger(channel *Channel, scavenger_chan chan *User, scavenger_idx int, user_state_chan chan *UserState) chan bool {
	close_chan := make(chan bool)
	go func() {
		defer func() {
			if err := recover(); err != nil {
				utils.Log.Println(err)
				debug.PrintStack()
			}
		}()

		var user *User
		var now int64
		var state *UserState

		user_list := make(map[string]*User, 1024)

		for {
			select {
			case <-close_chan:
				utils.Log.Printf("Channel [%s] Scavenger [%d] has quit...\n", channel.Name, scavenger_idx)
				close_chan <- true
				return

			case user = <-scavenger_chan:
				utils.Log.Printf("Channel [%s] Scavenger [%d] got user: %s\n", channel.Name, scavenger_idx, user.ID)
				user_list[user.ID] = user

				user.SpinLock.Lock()
				user.Online = true
				user.SpinLock.Unlock()

				state = channel.UserStatePool.Get().(*UserState)
				state.ID = user.ID
				state.Tag = user.Tag
				state.State = true
				state.From = 0
				user_state_chan <- state

				atomic.AddUint64(&channel.UserCount, 1)

			case _ = <-wheel_seconds.After(5 * time.Second):
				if len(user_list) > 0 {
					for idx := range user_list {
						now = time.Now().Unix()
						user = user_list[idx]

						// clean user's resource and free memory
						if now-user.LastUpdate > DELAY_CLEAN_USER_RESOURCE {
							channel.DeleteUser(user.ID)
							delete(user_list, user.ID)
							atomic.AddUint64(&channel.UserCount, ^uint64(0))
							utils.Log.Printf("Scavenger [%d] clean user: %s\n", scavenger_idx, user.ID)
						}

						// generate online user list
						if now-user.LastUpdate > DELAY_USER_ONLINE {
							user.SpinLock.Lock()
							if user.Online == true {
								user.Online = false

								state = channel.UserStatePool.Get().(*UserState)
								state.ID = user.ID
								state.Tag = user.Tag
								state.State = false
								state.From = 0
								user_state_chan <- state
							}
							user.SpinLock.Unlock()
						}

						if now-user.LastUpdate <= DELAY_USER_ONLINE {
							user.SpinLock.Lock()
							if user.Online == false {
								user.Online = true

								state = channel.UserStatePool.Get().(*UserState)
								state.ID = user.ID
								state.Tag = user.Tag
								state.State = true
								state.From = 0
								user_state_chan <- state
							}
							user.SpinLock.Unlock()
						}
					}
				}
			}
		}
	}()

	return close_chan
}

func StartGlobalScavenger() chan bool {
	close_chan := make(chan bool)
	go func() {
		var channel *Channel

		for {
			select {
			case <-close_chan:
				utils.Log.Println("Global Scavenger has quite.")
				return

			case <-wheel_seconds.After(DELAY_CLEAN_CHANNEL_RESOURCE * time.Second):
				all_channel.RLock.RLock()

				for channel_name := range all_channel.Channels {
					channel = all_channel.Channels[channel_name]
					now := time.Now().Unix()
					if atomic.LoadUint64(&channel.UserCount) == 0 && now-channel.LastPostUpdate > DELAY_CHANNEL_POST {
						all_channel.Lock.Lock()
						channel.PrepareClose = true

						utils.Log.Printf("Channel [%s] will be removed.\n", channel_name)

						// 等待Channel相关的goroutine退出
						for idx := range channel.CloseChan {
							close_chan := channel.CloseChan[idx]
							close_chan <- true
							<-close_chan
							close(close_chan)
							close_chan = nil
						}

						// 关闭相关channel
						close(channel.MultiCastStage0Chan)
						close(channel.MultiCastStage1Chan)
						channel.MultiCastStage0Chan = nil
						channel.MultiCastStage1Chan = nil

						for idx := range channel.MultiCastStage2Chan {
							stage_chan := channel.MultiCastStage2Chan[idx]
							close(stage_chan)
							stage_chan = nil
						}

						for idx := range channel.ScavengerChan {
							scavenger_chan := channel.ScavengerChan[idx]
							close(scavenger_chan)
							scavenger_chan = nil
						}

						for idx := range channel.UserChan {
							user_state_chan := channel.UserChan[idx]
							close(user_state_chan)
							user_state_chan = nil
						}

						channel.Closed = true

						delete(all_channel.Channels, channel_name)
						utils.Log.Printf("Channel [%s] was removed.\n", channel_name)

						all_channel.Lock.Unlock()
					}
				}

				all_channel.RLock.RUnlock()
			}
		}
	}()

	return close_chan
}

package main

import (
	"encoding/json"
	"flag"
	"github.com/gorilla/mux"
	"github.com/shelmesky/message_service/handler"
	"github.com/shelmesky/message_service/lib"
	"github.com/shelmesky/message_service/utils"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"sync"
	"syscall"
	"time"
)

var (
	logger      *log.Logger
	signal_chan chan os.Signal // 处理信号的channel

	ConfigFile           = flag.String("config", "./config.json", "")
	LogFile              = flag.String("log_file", "./server.log", "logging file, default: server.log")
	ListenAddress        = flag.String("listen_address", "", "server listen on, default: 0.0.0.0:34569")
	ProfileListenAddress = flag.String("profile_listen_address", "", "server listen on, default: 0.0.0.0:34570")
	EnableServerProfile  = flag.Bool("enable_profile", true, "Start web profile interface (true or false).")
	LogToStdout          = flag.Bool("log_to_stdout", false, "Print log to standard output (true or false).")
	ServerDebug          = flag.Bool("server_debug", false, "Print debug information")
	ForceGCPeriod        = flag.Int("force_gc_period", 60, "Period of force GC, default: 60 seconds")
	ForceGC              = flag.Bool("force_gc", true, "Run runtime.GC in force")
	ForceFreeOSMemory    = flag.Bool("force_free_os_memory", false, "Run debug.FreeOSMemory in force")
	KeepAlive            = flag.Bool("keepalive", true, "Enable HTTP Keepalive, default: true")
	PollMessageSize      = flag.Int("poll_message_size", 50, "Size of the poll's message size")
	MQTTServerAddress    = flag.String("mqtt_server", "", "MQTT server address")
	MQTTServerPort       = flag.String("mqtt_server_port", "", "MQTT server address")
	MQTTServerEnable     = flag.Bool("mqtt_server_enable", false, "enable or disable forward message to MQTT server")

	Config lib.GlobalConfig
)

func init() {
	var log_file *os.File
	var err error

	Config.Lock = new(sync.Mutex)

	flag.Parse()
	ExtraInit()

	// set config in handler
	handler.Config = &Config

	if !(*LogToStdout) {
		// init logging
		log_file, err = os.OpenFile(*LogFile, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}
	} else {
		log_file = os.Stdin
	}

	logger = log.New(log_file, "Server: ", log.Ldate|log.Ltime|log.Lshortfile)
	utils.Log = logger
}

func Exist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}

func ExtraInit() {
	if *EnableServerProfile == true {
		Config.EnableServerProfile = true
	}

	if *ServerDebug == true {
		Config.ServerDebug = true
		handler.ServerDebug = true
	}

	if !Exist(*ConfigFile) {
		if *LogFile == "" {
			*LogFile = "./server.log"
		}
		Config.LogFile = *LogFile

		if *ListenAddress == "" {
			*ListenAddress = "0.0.0.0:54569"
		}
		Config.ListenAddress = *ListenAddress

		if *ProfileListenAddress == "" {
			*ProfileListenAddress = "0.0.0.0:54570"
		}
		Config.ProfileListenAddress = *ProfileListenAddress

		if *ForceGCPeriod == 0 {
			*ForceGCPeriod = 60
		}
		Config.ForceGCPeriod = *ForceGCPeriod

		Config.ForceGC = *ForceGC
		Config.ForceFreeOSMemory = *ForceFreeOSMemory

		Config.KeepAlive = *KeepAlive
		Config.PollMessageSize = *PollMessageSize
		Config.MQTTServerAddress = *MQTTServerAddress
		Config.MQTTServerPort = *MQTTServerPort
		Config.MQTTServerEnable = *MQTTServerEnable

	} else {
		data, err := ioutil.ReadFile(*ConfigFile)
		if err != nil {
			log.Println(err)
			return
		}

		err = json.Unmarshal(data, &Config)
		if err != nil {
			log.Println(err)
		}

		if *LogFile != "" {
			Config.LogFile = *LogFile
		}

		if *ListenAddress != "" {
			Config.ListenAddress = *ListenAddress
		}

		if *ProfileListenAddress != "" {
			Config.ProfileListenAddress = *ProfileListenAddress
		}

		if *ForceGCPeriod > 0 {
			Config.ForceGCPeriod = *ForceGCPeriod
		}

		if *ForceGC != false {
			Config.ForceGC = *ForceGC
		}

		if *ForceFreeOSMemory != false {
			Config.ForceFreeOSMemory = *ForceFreeOSMemory
		}

		if *KeepAlive != false {
			Config.KeepAlive = *KeepAlive
		}

		if *PollMessageSize > 0 {
			Config.PollMessageSize = *PollMessageSize
		}

		if *MQTTServerAddress != "" {
			Config.MQTTServerAddress = *MQTTServerAddress
		}

		if *MQTTServerPort != "" {
			Config.MQTTServerPort = *MQTTServerPort
		}

		if *MQTTServerEnable != false {
			Config.MQTTServerEnable = *MQTTServerEnable
		}
	}
}

// 信号回调
func signalCallback() {
	for s := range signal_chan {
		sig := s.String()
		logger.Println("Got Signal: " + sig)

		if s == syscall.SIGINT || s == syscall.SIGTERM {
			logger.Println("Server exit...")
			os.Exit(0)
		}
	}
}

func GC() {
	for {
		time.Sleep(time.Duration(Config.ForceGCPeriod) * time.Second)
		if Config.ForceGC == true {
			logger.Println("Start garbage collection...")
			runtime.GC()
			logger.Println("End garbage collection...")
		}
		if Config.ForceFreeOSMemory == true {
			logger.Println("Free Memory to OS...")
			debug.FreeOSMemory()
		}
	}
}

func main() {
	defer func() {
		if err := recover(); err != nil {
			logger.Println(err)
			debug.PrintStack()
		}
	}()

	runtime.GOMAXPROCS(runtime.NumCPU())

	go GC()

	// HOLD住POSIX SIGNAL
	signal_chan = make(chan os.Signal, 10)
	signal.Notify(signal_chan,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		syscall.SIGPIPE,
		syscall.SIGALRM,
		syscall.SIGPIPE,
		syscall.SIGBUS,
		syscall.SIGCHLD,
		syscall.SIGCONT,
		syscall.SIGFPE,
		syscall.SIGILL,
		syscall.SIGIO,
		syscall.SIGIOT,
		syscall.SIGPROF,
		syscall.SIGSEGV,
		syscall.SIGSTOP,
		syscall.SIGSYS,
		syscall.SIGTRAP,
		syscall.SIGURG,
		syscall.SIGUSR1,
		syscall.SIGUSR2)

	go signalCallback()

	// 启动性能调试接口
	if *EnableServerProfile == true {
		go func() {
			http.ListenAndServe(Config.ProfileListenAddress, nil)
		}()
	}

	router := mux.NewRouter()

	/*
		ReadTimeout和WriteTimeout
		必须大于长轮询等待的时间
		并且在rever proxy中要配置read timeout
		read timeout要超过长轮询等待的时间
	*/
	s := &http.Server{
		Addr:           Config.ListenAddress,
		Handler:        router,
		ReadTimeout:    60 * time.Second,
		WriteTimeout:   60 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	router.HandleFunc("/api/post", handler.MessagePostHandler).Methods("POST")
	router.HandleFunc("/api/post", handler.GlobalOptionsHandler).Methods("OPTIONS")

	router.HandleFunc("/api/poll", handler.MessagePollHandler).Methods("GET")
	router.HandleFunc("/api/poll", handler.MessageDeleteHandler).Methods("DELETE")
	router.HandleFunc("/api/poll", handler.GlobalOptionsHandler).Methods("OPTIONS")

	router.HandleFunc("/api/users", handler.OnlineUsersHandler).Methods("GET")
	router.HandleFunc("/api/users/simple", handler.OnlineUsersSimpleHandler).Methods("GET")
	router.HandleFunc("/api/users/tags", handler.OnlineUsersHandlerWithTag).Methods("GET")
	router.HandleFunc("/api/users/simple/tags", handler.OnlineUsersSimpleHandlerWithTag).Methods("GET")

	router.HandleFunc("/api/add/{channel_name}", handler.ChannelAddHandler).Methods("GET")

	router.HandleFunc("/api/sys/config", handler.SysConfigHandler).Methods("POST")
	router.HandleFunc("/api/sys/status", handler.SysStatusHandler).Methods("GET")

	s.SetKeepAlivesEnabled(Config.KeepAlive)

	logger.Printf("Server [PID: %d] listen on [%s]\n", os.Getpid(), Config.ListenAddress)
	logger.Fatal(s.ListenAndServe())
}

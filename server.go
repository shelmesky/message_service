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
	ForceGCPeriod        = flag.Int("force_gc_period", 0, "Period of force GC, default: 60 seconds")
	ForceGC              = flag.Bool("force_gc", false, "Run runtime.GC in force")
	ForceFreeOSMemory    = flag.Bool("force_free_os_memory", false, "Run debug.FreeOSMemory in force")
	KeepAlive            = flag.Bool("keepalive", true, "Enable HTTP Keepalive, default: true")
	PollMessageSize      = flag.Int("poll_message_size", 50, "Size of the poll's message size")

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
			*ListenAddress = "0.0.0.0:34569"
		}
		Config.ListenAddress = *ListenAddress

		if *ProfileListenAddress == "" {
			*ProfileListenAddress = "0.0.0.0:34570"
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
			if Config.ForceFreeOSMemory == true {
				logger.Println("Free Memory to OS...")
				debug.FreeOSMemory()
			}
			logger.Println("End garbage collection...")
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
		syscall.SIGPIPE)

	go signalCallback()

	// 启动性能调试接口
	if *EnableServerProfile == true {
		go func() {
			http.ListenAndServe(Config.ProfileListenAddress, nil)
		}()
	}

	router := mux.NewRouter()

	s := &http.Server{
		Addr:           Config.ListenAddress,
		Handler:        router,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	router.HandleFunc("/api/post", handler.MessagePostHandler).Methods("POST")
	router.HandleFunc("/api/poll", handler.MessagePollHandler).Methods("GET")
	router.HandleFunc("/api/post", handler.GlobalOptionsHandler).Methods("POST")
	router.HandleFunc("/api/poll", handler.GlobalOptionsHandler).Methods("OPTIONS")
	router.HandleFunc("/api/add/{channel_name}", handler.ChannelAddHandler).Methods("GET")
	router.HandleFunc("/api/sys/config", handler.SysConfigHandler).Methods("POST")

	utils.Log.Println("######", Config.KeepAlive)
	s.SetKeepAlivesEnabled(Config.KeepAlive)

	logger.Printf("Server [PID: %d] listen on [%s]\n", os.Getpid(), Config.ListenAddress)
	logger.Fatal(s.ListenAndServe())
}

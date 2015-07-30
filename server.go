package main

import (
	"encoding/json"
	"flag"
	"github.com/gorilla/mux"
	"github.com/shelmesky/message_service/handler"
	"github.com/shelmesky/message_service/utils"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
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

	Config GlobalConfig
)

type GlobalConfig struct {
	LogFile              string `json:"log_file"`
	ListenAddress        string `json:"listen_address"`
	ProfileListenAddress string `json:"profile_listen_address"`
	EnableServerProfile  bool
	LogToStdout          bool
	ServerDebug          bool
}

func init() {
	var log_file *os.File
	var err error

	flag.Parse()
	ExtraInit()

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
		time.Sleep(60 * time.Second)
		logger.Println("Start GC now...")
		runtime.GC()
		logger.Println("End GC now...")
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

	// 每60秒GC一次
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

	router.HandleFunc("/api/{channel}/post", handler.MessagePostHandler).Methods("POST")
	router.HandleFunc("/api/{channel}/poll/{user_id}", handler.MessagePollHandler).Methods("GET")

	s.SetKeepAlivesEnabled(false)

	logger.Printf("Server [PID: %d] listen on [%s]\n", os.Getpid(), Config.ListenAddress)
	logger.Fatal(s.ListenAndServe())
}

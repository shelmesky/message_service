package main

import (
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

const (
	SERVER_LISTEN       = "0.0.0.0:34569"
	SERVER_PERF_LISTEN  = "0.0.0.0:34570"
	ENABLE_PERF_PROFILE = true
	LOG_FILE            = "server.log"
)

var (
	logger      *log.Logger
	signal_chan chan os.Signal // 处理信号的channel
)

func init() {
	// init logging
	log_file, err := os.OpenFile(LOG_FILE, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	logger = log.New(log_file, "Server: ", log.Ldate|log.Ltime|log.Lshortfile)
	ExtraInit()
}

func ExtraInit() {
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

func MessagePostHandler(w http.ResponseWriter, r *http.Request) {
}

func MessagePollHandler(w http.ResponseWriter, r *http.Request) {
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
	if ENABLE_PERF_PROFILE == true {
		go func() {
			http.ListenAndServe(SERVER_PERF_LISTEN, nil)
		}()
	}

	http.HandleFunc("/api/message/post", MessagePostHandler)
	http.HandleFunc("/api/message/poll", MessagePollHandler)

	s := &http.Server{
		Addr:           SERVER_LISTEN,
		Handler:        nil,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	s.SetKeepAlivesEnabled(false)

	logger.Printf("Server [PID: %d] listen on [%s]\n", os.Getpid(), SERVER_LISTEN)
	logger.Fatal(s.ListenAndServe())
}

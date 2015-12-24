package lib

import (
	"sync"
)

type GlobalConfig struct {
	LogFile              string `json:"log_file"`
	ListenAddress        string `json:"listen_address"`
	ProfileListenAddress string `json:"profile_listen_address"`
	ServerDebug          bool   `json:"server_debug"`
	ForceGCPeriod        int    `json:"force_gc_period"`
	ForceGC              bool   `json:"force_gc"`
	ForceFreeOSMemory    bool   `json:"force_free_os_memory"`
	KeepAlive            bool   `json:"keepalive"`
	PollMessageSize      int    `json:"poll_message_size"`
	MQTTServerAddress    string `json:"mqtt_server"`
	MQTTServerEnable     bool   `json:"mqtt_server_enable"`
	EnableServerProfile  bool
	LogToStdout          bool
	Lock                 *sync.Mutex
}

#!/bin/bash

./message_service -log_to_stdout=true -server_debug=true -force_gc=true -force_gc_period=60 -force_free_os_memory=true -keepalive=true -mqtt_server=127.0.0.1 -mqtt_server_port=1883 -mqtt_server_enable=false

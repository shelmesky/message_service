#!/bin/bash

./message_service -log_to_stdout=true -server_debug=false -force_gc=true -force_gc_period=60 -force_free_os_memory=false

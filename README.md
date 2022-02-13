# Iron Carrier

A tool to synchronize files in multiple computers, written in Rust
This is not production ready, it is intended for personal use and it is NOT TESTED YET

# How to use

Each computer needs to have a daemon running and the settings in the configuration file.  
Default location of the configuration file is **~/.config/iron_carrier** but this can be override
with the **--config** flag.  

The minimum configuration file consists of a map of folders to be in sync in those computers.  
Each folder is aliased in the configuration file, so the folder structure can be different in different computers

# Ignoring files
Each root folder can have a **.ignore** file with glob patterns to ignore files, the file must be
always located at the root.  
This file is **NOT** synchronized between peers, you need to write the file manually for every peer
you want to synchronize


# Configuration File
```toml
# 10 character long unique id for this node
node_id = "my_node_id"

# listening port, defaults to 25230
port = 25230  

# listen to events in real time, defaults to true
enable_file_watcher = true

# time to delay watcher events, in seconds, defaults to 10
# useful to avoid sending the same file a lot of times
delay_watcher_events = 10


# Service discovery is active by default, it is possible to disable it and specify a list of  peers
enable_service_discovery = true

# Path location
log_path = "~/.iron_carrier/iron_carrier.log"

# Optional list of peers to sync
peers = [
    "otherpeer:8091"
]

# Paths to synchronize
[paths]
my_docs = "/home/my_user/Documents"
some_pictures = "/home/my_user/Pictures"
```


# Motivation
I have decided to implement this project as a way to learn Rust. I strongly believe the best way to learn a programming language is to use it in a real life scenario  
Rust have features that look very simple to understand, but are actually very complex, they are even harder to understand if you come from high level languages, like C# and JS.   


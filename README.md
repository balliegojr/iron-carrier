# Iron Carrier

A tool to synchronize files in multiple computers, written in Rust
This is not production ready, it is intended for personal use and it is NOT TESTED YET

# How to use

Each computer needs to have a daemon running and fully configurated.  
The configuration consists of a list of peers (ip:port) and a list of folders to be in sync in those computers.  
Each folder is aliased in the configuration file, so the folder structure can be different in different computers

## Example
### Peer A
```toml
peers = [ "192.168.1.10:8090" ]

[paths]
my_docs = "/home/myuser/Documents"
service_x_conf = "/etc/servicex/"
```

### Peer B
```toml
peers = [ "192.168.1.11:8090" ]

[paths]
my_docs = "/mnt/backup_hdd/documents_backup"
service_x_conf = "/etc/servicex/"
```

Notice that **my_docs** have different paths, but **service_x_conf** have the path on both peers.


# Configuration
```toml
# listening port, defaults to 8090
port = 8090 

# listen to events in real time, defaults to true
enable_file_watcher = true

# time to debouce real time events, in seconds, defaults to 10
debounce_events_seconds = 10

# List of peers to sync
peers = [
    "127.0.0.1:8091"
]

# List of paths to watch
[paths]
a = "./samples/peer_a"


```

# Planned features
- track deleted files for a given period of time, or sincronization with all peers
- Cron for sincronization
- rsync protocol for file transfer
- zero-conf for network sincronization
- Windows and Mac compatibility


# Motivation
I have decided to implement this project as a way to learn Rust. I strongly believe the best way to learn a programing language is to use it in a real life scenario  
Rust have features that look very simple to understand, but are actually very complex, they are even harder to understand if you come from high level languages, like C# and JS.   

I have read an advice somewhere (Reddit or Stackoverflow) that I'm very sad that I can't point to the autor. But it said: when in doubt, clone it. 

You don't need to write the best performant code right away, just code... refactor later. This advice is specially good if you find yourself losing a battle against lifetimes

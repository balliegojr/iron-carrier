# Iron Carrier

Peer to Peer file synchronization, written in Rust.  
This is not production ready, it is intended for personal use and it is NOT TESTED YET

# How to use

The minimum necessary configuration is the list of directories to be synchronized:
```toml
[storages]
my_storage = "/path/to/my_storage" 
some_other_storage = "/some/other/path"
```

Save the configuration file in the default location **$HOME/.config/iron-carrier/config.toml** or use **--config** flag to specify a path.  
With the config file set up, you just need to run the binary. It is possible to use **--daemon** to keep the binary running.

Directories will be synchronized by the name given to them, **my_storage** and **some_other_storage** in the example above.

## Ignoring files
Each root folder can have a **.ignore** file with glob patterns to ignore files, the file must be
always located at the root of the storage


# Configuration File
These are all the options accepted by the configuration file, with the default values

```toml
# Specify the id for this machine, must be unique in the group
node_id = "my_computer_name"

# Logical group to limit which nodes talk with each other.
# Each node will only synchronize within the same group
group = "my_group"

# Port to list to connections
port = 25230

# Optional list of nodes to connect to.
# Only necessary if service discovery is not enabled
peers = ["192.168.1.10:25230"]

# Enable file watcher events when running daemon mode
enable_file_watcher = true

# When a file change is detected, wait for # seconds to trigger the synchronization
# This is usefull to avoid too frequent synchronizations when editing a file
delay_watcher_events = 4

# Cron schedule to execute a full sync
schedule_sync = "0 0 0 0 0"

# Enable service discovery in the same network (mDNS queries on 5353/UDP)
enable_service_discovery = true

# Override the changes log path
log_path = "$HOME/.config/iron-carrier/iron-carrier.db"

# Key used for network encryption 
encryption = true

# It is also possible to use a pre defined key that will be used to construct the final encryption key
# encryption = "some super safe key"

# Maximum number of parallel transfers
max_parallel_transfers = 4

# List of storages to synchronize
[storages]
# Simple path format
"my storage" = "path/to/directory"

# Advanced configuration
"my storage" = { 
  path = "path/to/directory", 

  # Override the default config
  enable_watcher = true,
}
```

# Philosophy

I believe that good software should run on a toaster. 

Ok, we can put 12 CPUS and 128 GB of ram in a toaster, but my point is, it should run on something like a raspberry, or any machine with low resources.

I also have a thing for distributed systems.

With that in mind, the design decisions/requirements for this software are:
- No Servers. 
- Minimum resource usage.

These two design decisions impose limits on what this software can do, or at least how it does.  
The synchronization process is peer to peer, whenever two or more nodes need to synchronize, a leader election protocol is used to decide who leads that session.
Each session will, potentially, have a different leader.

This allows any number of nodes to participate in the synchronization, and also to have a mixed set of "servers" (processes running as daemons) and ad-hoc nodes.

However, this also limits how and which changes are tracked, since the process may not be running when a file change happens. 
For processes running in daemon mode, with file watcher enabled, it is possible to track when a file is deleted, renamed or moved. This is done using a log inside a sqlite file.
Otherwise, the only information available is the current state of the file system.

A file deleted or moved when the daemon is not running, will be recreated in the next sync.

## Synchronization process

To minimize resource usage, the process is done per storage, one at time.

The first step is to build an index with the current state of the file system across all nodes. The index contains the relative file path, size and timestamps. Nodes in daemon mode will also include deleted files, and moved files with current and previous names.

At this moment, a shallow diff is done, whithout looking at the file contents. If timestamps or size diverge on different nodes, the most recent change will be propagated to the other nodes. If the most recent change for a file is a deletion or move event, that change will be propagated to other nodes.

Now it is time to send file contents to other nodes.  

- If it is a new file, the node that has the file will open the file in reading mode and send the file in chunks simultaneously to all the nodes that need it.
- If it is an existing file, each node will build an index of the file, by calculating a hash of every non overlaping chunk, so only the divergent chunks are exchanged between the nodes.

## Multiple users in the same network
Groups are useful when more than one user have iron carrier running in the same network with service discovery enabled.  
A unique group name will prevent from trying to synchronize with different users.

It is also possible to disable service discovery and use the **peers** configuration to list the nodes to connect to.

## Network encryption
This software is intended to be used in trusted networks, in machines with low resources, for this reason encryption can be disabled.
If privacy is a concern, encryption can be enabled by setting the **encryption** property to encrypt all network communication.

The protocol used for encryption is **XChaCha20Poly1305**.

I am not a security expert, this software is not audited, if security is a concern or if you suspect the presence of malicious actors in the network,  
do not use this software.

If you are a security expert and know of ways this software can be improved, please contact me or open a discussion in github.


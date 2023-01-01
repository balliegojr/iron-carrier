# Iron Carrier

A tool to synchronize files in between computers on a peer to peer manner, written in Rust
This is not production ready, it is intended for personal use and it is NOT TESTED YET

# How it works
Synchronization happens without any server or orchestrator, files never leave the local network.
The communication is NOT ENCRYPTED (yet), so it should be used only when connected to a trusted network.  

Since there is no server or orchestrator, the service need to be running on both machines at the same time to be able to synchronize. 
Every time the service starts, it will attempt to carry a full synchronization with every other available peer. After the full synchronization, if `enable_file_watcher = true`, the service will watch every listed folder for file changes and propagate them in [near] real time.

The full process has the following steps:
1. For every monitored folder (internally called as `storage`), list every file and calculate a hash to represent the storage state, only the file `path`, `size` and `modification or deleted` information are used to calculate this hash.  
2. Query the same information from every other available peer.  
3. If there are storages with different hashes, we proceed for every storage
4. Build an index with all the files for this storage and request the same information from the other peers with divergent storage hash.  
5. Build a consolidated index containing every file from all the indexes.
6. For every file, decide what to do based on the file attributes, considering the most recent one. Only 2 actions can take place:
  1. Delete the file (local or remote) 
  2. Transfer an existing file (by requesting or propagating to other peers)
7. Repeat from step `3` until there are no storages left
8. Go to step `1`, there should be no storages with different hashes this time

## Handling file transfer
When sending or requesting a file, the content is divided in chunks and an index with the hash of every chunk is exchanged with the destination peer, then the peer can do the same process and compare both indexes and send a reply listing only the chunks that are divergent and need to be synchronized (this process only happens when the same file exist on both peers), then the initial peer can transfer only the chunks that are necessary. This process is similar to what `rsync` does, but `rsync` uses an algorithm called [rolling checksum](https://rsync.samba.org/tech_report/node3.html) which is more advanced.  
This means that, if the file content shifts (by prepending or deleting at the begging of the file), the whole file will be synchronized again, if the changes is in place or appended to the end of the file, only a small chunk will be transferred.

## Handling deleted files and failed writes
This system uses a log to keep track of the actions taken by it, or while the files are being watched. When a file is deleted or moved, the event is appended to a log before being propagated to other peers. When the full synchronization takes place, the entries for deleted files are read from the log and appended to the file index, this way deleted files are not recreated on the next synchronization, otherwise a peer would not now that if the other peer never had a file or if the file was deleted.  

The same log keeps track of file writes, with two events, one when the transfer starts and one when transfer finishes successfully. This prevents propagation of failed writes for the next full synchronization by removing the partially written file from the index and requesting it again.  

The caveat of using a log file instead a database, is that if the service is stopped, there is no way to know what changes took place. Consider the following scenario, there are two peers A and B, both have the same file already synchronized. The service running on peer A is manually stopped and after that the file is deleted or moved. When the service is started again on peer A, there is no record that the file ever existed in the original path, so peer B will send the file to peer A. This means that deleted files will be recreated and moved files will be duplicated if the change takes place when the service is not running.  


# How to use

Each computer needs to have a daemon running and the settings in the configuration file.  
Default location of the configuration file is **~/.config/iron_carrier** but this can be override
with the **--config** flag.  

The minimum configuration file consists of a map of folders to be in sync in those computers.  
Each folder is aliased in the configuration file, so the folder structure can be different in different computers

## Group of nodes
It is possible to provide a `group` in the config file. Each node will only communicate with
another node in the same group. 

## Ignoring files
Each root folder can have a **.ignore** file with glob patterns to ignore files, the file must be
always located at the root.  
This file is **NOT** synchronized between peers, you need to write the file manually for every peer
you want to synchronize


## Configuration File
TODO: new config example


# Motivation
I have decided to implement this project as a way to learn Rust. I strongly believe the best way to learn a programming language is to use it in a real life scenario  
Rust have features that look very simple to understand, but are actually very complex, they are even harder to understand if you come from high level languages, like C# and JS.   


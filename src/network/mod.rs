use serde::{Serialize, Deserialize };

use crate::fs::FileInfo;

pub mod server;
pub mod peer;

type CommandResult = Result<Command, Box<bincode::ErrorKind>>;
type ResponseResult = Result<Response, Box<bincode::ErrorKind>>;

const BUFFER_SIZE: usize = 1024;

#[derive(Serialize,Deserialize, Debug)]
pub enum Command {
    Ping,
    Close,
    UnsyncedFileList(Vec<(String, u64)>),
    PrepareFile(String, FileInfo),
}

#[derive(Serialize,Deserialize, Debug, PartialEq)]
pub enum Response {
    Pong,
    FileList(Vec<(String, Vec<FileInfo>)>),
    Success
}
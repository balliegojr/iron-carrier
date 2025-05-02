#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

#[derive(Debug, Clone, Default)]
pub struct Metadata {
    len: u64,
    created: u64,
    modified: u64,
    permissions: u32,
    is_dir: bool,
}

impl Metadata {
    pub(super) fn new(metadata: std::fs::Metadata) -> Self {
        Self {
            len: metadata.len(),
            is_dir: metadata.is_dir(),
            permissions: get_permissions(&metadata),
            created: metadata
                .created()
                .map(crate::time::system_time_to_secs)
                .unwrap_or(0),
            modified: metadata
                .modified()
                .map(crate::time::system_time_to_secs)
                .unwrap_or(0),
        }
    }

    pub fn is_dir(&self) -> bool {
        self.is_dir
    }

    pub fn permissions(&self) -> u32 {
        self.permissions
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn created_as_secs(&self) -> u64 {
        self.created
    }

    pub fn modified_as_secs(&self) -> u64 {
        self.modified
    }
}

#[cfg(test)]
pub struct MetadataB {
    inner: Metadata,
}

#[cfg(test)]
impl MetadataB {
    pub fn new() -> Self {
        Self {
            inner: Metadata::default(),
        }
    }
    pub fn dir() -> Self {
        Self {
            inner: Metadata {
                is_dir: true,
                ..Metadata::default()
            },
        }
    }

    pub fn len(mut self, len: u64) -> Self {
        self.inner.len = len;
        self
    }
    pub fn created(mut self, created: u64) -> Self {
        self.inner.created = created;
        self
    }
    pub fn modified(mut self, modified: u64) -> Self {
        self.inner.modified = modified;
        self
    }
    pub fn permissions(mut self, permissions: u32) -> Self {
        self.inner.permissions = permissions;
        self
    }
    pub fn build(self) -> Metadata {
        self.inner
    }
}

#[cfg(unix)]
pub fn get_permissions(metadata: &std::fs::Metadata) -> u32 {
    metadata.permissions().mode()
}

#[cfg(not(unix))]
pub fn get_permissions(metadata: &std::fs::Metadata) -> u32 {
    //TODO: figure out how to handle windows permissions
    0
}

#[cfg(unix)]
pub fn set_file_permissions(path: &Path, perm: u32) -> std::io::Result<()> {
    let perm = std::fs::Permissions::from_mode(perm);
    std::fs::set_permissions(path, perm)
}

#[cfg(not(unix))]
pub fn set_file_permissions(path: &Path, perm: u32) -> std::io::Result<()> {
    //TODO: figure out how to handle windows permissions
    Ok(())
}

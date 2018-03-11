use super::libc;
use std::path::Path;
use std::fs;
use std::io;
use std::io::prelude::*;
use std::fs::OpenOptions;

// Recursively constructs a directory tree
pub fn mkdir(path: &Path) -> io::Result<()> {
    let parent = path.parent();
    match parent {
        Some(parent) => {
            if !parent.is_dir() {
                mkdir(parent)?
            }
            if !path.is_dir() {
                fs::create_dir(path)
            } else {
                Ok(())
            }
        }
        None => Ok(()),
    }
}

pub fn drop_cache() {
    // 'echo 3 >/proc/sys/vm/drop_caches'
    sync_all();
    let mut drop_cache_file = OpenOptions::new()
        .write(true)
        .open("/proc/sys/vm/drop_caches")
        .expect("failed to open drop_caches but we should be root");
    drop_cache_file
        .write_all(b"3\n")
        .expect("failed to write to drop_caches but we should be root");
}

// Wrapper around unsafe libc::sync
pub fn sync_all() {
    unsafe {
        libc::sync();
    }
}

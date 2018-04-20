use super::libc;
use std::fs;
use std::fs::OpenOptions;
use std::io;
use std::io::prelude::*;
use std::path::Path;

// Recursively constructs a directory tree
pub fn mkdir<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let parent = path.as_ref().parent();
    match parent {
        Some(parent) => {
            if !parent.is_dir() {
                mkdir(parent)?
            }
            if !path.as_ref().is_dir() {
                fs::create_dir(&path)
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

pub enum Filesystem {
    Ext2,
    Ext4,
    Ext4NoJournal,
    Xfs,
    Btrfs,
    F2fs
}

pub fn mkfs(device: &str, fs: &Filesystem) {
    use std::process::Command;
    match *fs {
        Filesystem::Ext2 => {
            if !Command::new("mkfs.ext2")
                .args(&[&"-F", device])
                .status()
                .expect("failed to run `mkfs.ext2`")
                .success() {
                    panic!("failed to mkfs.ext2 {}", device);
                }
        },
        Filesystem::Ext4 => {
            if !Command::new("mkfs.ext4")
                .args(&[&"-F", device])
                .status()
                .expect("failed to run `mkfs.ext4`")
                .success() {
                    panic!("failed to mkfs.ext4 {}", device);
                }
        },
        Filesystem::Ext4NoJournal => {
            if !Command::new("mkfs.ext4")
                .args(&[&"-F", device])
                .status()
                .expect("failed to run `mkfs.ext4`")
                .success() {
                    panic!("failed to mkfs.ext4 {}", device);
                }
            if !Command::new("tune2fs")
                .args(&[&"-o", &"journal_data_writeback", device])
                .status()
                .expect("failed to run `tune2fs`")
                .success() {
                    panic!("failed to tune2fs {}", device);
                }
            if !Command::new("tune2fs")
                .args(&[&"-O", &"^has_journal", device])
                .status()
                .expect("failed to run `tune2fs`")
                .success() {
                    panic!("failed to tune2fs {}", device);
                }
            if !Command::new("e2fsck")
                .args(&[&"-F", device])
                .status()
                .expect("failed to run `e2fsck`")
                .success() {
                    panic!("failed to e2fsck {}", device);
                }
        },
        Filesystem::Xfs => {
            if !Command::new("mkfs.xfs")
                .args(&[&"-f", device])
                .status()
                .expect("failed to run `mkfs.xfs`")
                .success() {
                    panic!("failed to mkfs.xfs {}", device);
                }
        },
        Filesystem::F2fs => {
            if !Command::new("mkfs.f2fs")
                .args(&[&"-f", device])
                .status()
                .expect("failed to run `mkfs.f2fs`")
                .success() {
                    panic!("failed to mkfs.f2fs {}", device);
                }
        },
        Filesystem::Btrfs => {
            if !Command::new("mkfs.btrfs")
                .args(&[&"-f", device])
                .status()
                .expect("failed to run `mkfs.btrfs`")
                .success() {
                    panic!("failed to mkfs.btrfs {}", device);
                }
        },
    }
}

impl ToString for Filesystem {
    fn to_string(&self) -> String {
        match *self {
            Filesystem::Ext2 => String::from("ext2"),
            Filesystem::Ext4 => String::from("ext4"),
            Filesystem::Ext4NoJournal => String::from("ext4-no-journal"),
            Filesystem::Xfs => String::from("xfs"),
            Filesystem::Btrfs => String::from("btrfs"),
            Filesystem::F2fs => String::from("f2fs"),
        }
    }
}

use super::nix;
use super::libc;
use super::util;
use super::statistics::Stats;
use std::mem;
use nix::fcntl::OFlag;
use nix::sys::stat::Mode;
use std::os::unix::io::RawFd;
use std::time::Instant;
use std::path::Path;
use std::fs;
use std::fmt;
use std::io;
use std::sync::RwLock;

pub struct Open {
    pub stats: RwLock<Stats>,
}

impl Open {
    pub fn new() -> Open {
        Open {
            stats: RwLock::new(Stats::new()),
        }
    }

    pub fn run<P: ?Sized + nix::NixPath>(
        &mut self,
        path: &P,
        oflag: OFlag,
        mode: Mode,
    ) -> nix::Result<RawFd> {
        let mut stats = self.stats.write().unwrap();
        let start = Instant::now();
        match nix::fcntl::open(path, oflag, mode) {
            Ok(fd) => {
                stats.record(start.elapsed(), 0);
                Ok(fd)
            }
            Err(e) => Err(e),
        }
    }
}

pub struct Close {
    pub stats: RwLock<Stats>,
}

impl Close {
    pub fn new() -> Close {
        Close {
            stats: RwLock::new(Stats::new()),
        }
    }

    pub fn run(&mut self, fd: RawFd) -> nix::Result<()> {
        let mut stats = self.stats.write().unwrap();
        let start = Instant::now();
        match nix::unistd::close(fd) {
            Ok(()) => {
                stats.record(start.elapsed(), 0);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

pub struct Fsync {
    pub stats: RwLock<Stats>,
}

impl Fsync {
    pub fn new() -> Fsync {
        Fsync {
            stats: RwLock::new(Stats::new()),
        }
    }

    pub fn run(&mut self, fd: RawFd) -> nix::Result<()> {
        let mut stats = self.stats.write().unwrap();
        let start = Instant::now();
        match nix::unistd::fsync(fd) {
            Ok(()) => {
                stats.record(start.elapsed(), 0);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

pub struct Sync {
    pub stats: RwLock<Stats>,
}

impl Sync {
    pub fn new() -> Sync {
        Sync {
            stats: RwLock::new(Stats::new()),
        }
    }

    pub fn run(&mut self) {
        let mut stats = self.stats.write().unwrap();
        let start = Instant::now();
        util::sync_all();
        stats.record(start.elapsed(), 0);
    }
}

pub struct Read {
    pub stats: RwLock<Stats>,
}

impl Read {
    pub fn new() -> Read {
        Read {
            stats: RwLock::new(Stats::new()),
        }
    }

    pub fn run(&mut self, fd: RawFd, buf: &mut [u8]) -> nix::Result<usize> {
        let mut stats = self.stats.write().unwrap();
        let start = Instant::now();
        match nix::unistd::read(fd, buf) {
            Ok(bytes_read) => {
                stats.record(start.elapsed(), bytes_read);
                Ok(bytes_read)
            }
            Err(e) => Err(e),
        }
    }
}

pub struct Write {
    pub stats: RwLock<Stats>,
}

impl Write {
    pub fn new() -> Write {
        Write {
            stats: RwLock::new(Stats::new()),
        }
    }

    pub fn run(&mut self, fd: RawFd, buf: &[u8]) -> nix::Result<usize> {
        let mut stats = self.stats.write().unwrap();
        let start = Instant::now();
        match nix::unistd::write(fd, buf) {
            Ok(bytes_written) => {
                stats.record(start.elapsed(), bytes_written);
                Ok(bytes_written)
            }
            Err(e) => Err(e),
        }
    }
}

pub struct Unlink {
    pub stats: RwLock<Stats>,
}

impl Unlink {
    pub fn new() -> Unlink {
        Unlink {
            stats: RwLock::new(Stats::new()),
        }
    }

    pub fn run<P: ?Sized + nix::NixPath>(&mut self, path: &P) -> nix::Result<()> {
        let mut stats = self.stats.write().unwrap();
        let start = Instant::now();
        match nix::unistd::unlink(path) {
            Ok(()) => {
                stats.record(start.elapsed(), 0);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

pub struct Rename {
    pub stats: RwLock<Stats>,
}

impl Rename {
    pub fn new() -> Rename {
        Rename {
            stats: RwLock::new(Stats::new()),
        }
    }

    pub fn run<P: AsRef<Path>, Q: AsRef<Path>>(
        &mut self,
        from_path: &P,
        to_path: &Q,
    ) -> io::Result<()> {
        let mut stats = self.stats.write().unwrap();
        let start = Instant::now();
        match fs::rename(from_path, to_path) {
            Ok(()) => {
                stats.record(start.elapsed(), 0);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

pub struct ReadDir {
    pub stats: RwLock<Stats>,
}

impl ReadDir {
    pub fn new() -> ReadDir {
        ReadDir {
            stats: RwLock::new(Stats::new()),
        }
    }

    pub fn run<P: AsRef<Path> + ::std::fmt::Debug>(&mut self, path: P) -> io::Result<()> {
        let mut stats = self.stats.write().unwrap();
        let readdir = fs::read_dir(path)?;
        let start = Instant::now();
        let mut bytes = 0;
        for entry in readdir {
            match entry {
                Ok(entry) => bytes += entry.file_name().len() + mem::size_of::<libc::dirent>() - 1,
                Err(e) => return Err(e),
            }
        }
        stats.record(start.elapsed(), bytes);
        Ok(())
    }
}

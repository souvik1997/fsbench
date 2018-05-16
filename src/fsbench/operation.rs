use super::libc;
use super::nix;
use super::statistics::Stats;
use super::util;
use nix::fcntl::OFlag;
use nix::sys::stat::Mode;
use std::fs;
use std::io;
use std::mem;
use std::os::unix::io::RawFd;
use std::path::Path;
use std::sync::RwLock;
use std::time::Instant;

pub trait Operation {
    fn get_stats(&self) -> Stats;
}

pub struct Open {
    stats: RwLock<Stats>,
}

fn get_iowait() -> usize {
    use std::fs::File;
    use std::io::Read;
    use std::str;
    let f = File::open("/proc/stat").expect("failed to open /proc/stats");
    let split_string = vec![String::new()];
    let first_line_bytes = f.bytes().take_while(|c| {
        match c {
            &Ok(ref ch) => *ch != '\n' as u8,
            &_ => false,
        }
    }).map(|c| c.unwrap()).collect::<Vec<u8>>();
    let first_line = str::from_utf8(&first_line_bytes).expect("failed to parse bytes as utf8");
    let mut split = first_line.split_whitespace();
    let iowait: Option<usize> = split.nth(5).and_then(|s| s.parse().ok()); // 6th column is iowait
    iowait.expect("failed to read iowait")
}

impl Open {
    pub fn new() -> Open {
        Open {
            stats: RwLock::new(Stats::new()),
        }
    }

    pub fn run<P: ?Sized + nix::NixPath>(&mut self, path: &P, oflag: OFlag, mode: Mode) -> nix::Result<RawFd> {
        let mut stats = self.stats.write().unwrap();
        let start_iowait = get_iowait();
        let start = Instant::now();
        match nix::fcntl::open(path, oflag, mode) {
            Ok(fd) => {
                let elapsed = start.elapsed();
                stats.record(elapsed, 0, get_iowait() - start_iowait);
                Ok(fd)
            }
            Err(e) => Err(e),
        }
    }
}

impl Operation for Open {
    fn get_stats(&self) -> Stats {
        self.stats.read().unwrap().clone()
    }
}

pub struct Close {
    stats: RwLock<Stats>,
}

impl Close {
    pub fn new() -> Close {
        Close {
            stats: RwLock::new(Stats::new()),
        }
    }

    pub fn run(&mut self, fd: RawFd) -> nix::Result<()> {
        let mut stats = self.stats.write().unwrap();
        let start_iowait = get_iowait();
        let start = Instant::now();
        match nix::unistd::close(fd) {
            Ok(()) => {
                let elapsed = start.elapsed();
                stats.record(elapsed, 0, get_iowait() - start_iowait);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

impl Operation for Close {
    fn get_stats(&self) -> Stats {
        self.stats.read().unwrap().clone()
    }
}

pub struct Fsync {
    stats: RwLock<Stats>,
}

impl Fsync {
    pub fn new() -> Fsync {
        Fsync {
            stats: RwLock::new(Stats::new()),
        }
    }

    pub fn run(&mut self, fd: RawFd) -> nix::Result<()> {
        let mut stats = self.stats.write().unwrap();
        let start_iowait = get_iowait();
        let start = Instant::now();
        match nix::unistd::fsync(fd) {
            Ok(()) => {
                let elapsed = start.elapsed();
                stats.record(elapsed, 0, get_iowait() - start_iowait);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

impl Operation for Fsync {
    fn get_stats(&self) -> Stats {
        self.stats.read().unwrap().clone()
    }
}

pub struct Sync {
    stats: RwLock<Stats>,
}

impl Sync {
    pub fn new() -> Sync {
        Sync {
            stats: RwLock::new(Stats::new()),
        }
    }

    pub fn run(&mut self) {
        let mut stats = self.stats.write().unwrap();
        let start_iowait = get_iowait();
        let start = Instant::now();
        util::sync_all();
        let elapsed = start.elapsed();
        stats.record(elapsed, 0, get_iowait() - start_iowait);
    }
}

impl Operation for Sync {
    fn get_stats(&self) -> Stats {
        self.stats.read().unwrap().clone()
    }
}

pub struct Read {
    stats: RwLock<Stats>,
}

impl Read {
    pub fn new() -> Read {
        Read {
            stats: RwLock::new(Stats::new()),
        }
    }

    pub fn run(&mut self, fd: RawFd, buf: &mut [u8]) -> nix::Result<usize> {
        let mut stats = self.stats.write().unwrap();
        let start_iowait = get_iowait();
        let start = Instant::now();
        match nix::unistd::read(fd, buf) {
            Ok(bytes_read) => {
                let elapsed = start.elapsed();
                stats.record(elapsed, bytes_read, get_iowait() - start_iowait);
                Ok(bytes_read)
            }
            Err(e) => Err(e),
        }
    }
}

impl Operation for Read {
    fn get_stats(&self) -> Stats {
        self.stats.read().unwrap().clone()
    }
}

pub struct Write {
    stats: RwLock<Stats>,
}

impl Write {
    pub fn new() -> Write {
        Write {
            stats: RwLock::new(Stats::new()),
        }
    }

    pub fn run(&mut self, fd: RawFd, buf: &[u8]) -> nix::Result<usize> {
        let mut stats = self.stats.write().unwrap();
        let start_iowait = get_iowait();
        let start = Instant::now();
        match nix::unistd::write(fd, buf) {
            Ok(bytes_written) => {
                let elapsed = start.elapsed();
                stats.record(elapsed, bytes_written, get_iowait() - start_iowait);
                Ok(bytes_written)
            }
            Err(e) => Err(e),
        }
    }
}

impl Operation for Write {
    fn get_stats(&self) -> Stats {
        self.stats.read().unwrap().clone()
    }
}

pub struct Unlink {
    stats: RwLock<Stats>,
}

impl Unlink {
    pub fn new() -> Unlink {
        Unlink {
            stats: RwLock::new(Stats::new()),
        }
    }

    pub fn run<P: ?Sized + nix::NixPath>(&mut self, path: &P) -> nix::Result<()> {
        let mut stats = self.stats.write().unwrap();
        let start_iowait = get_iowait();
        let start = Instant::now();
        match nix::unistd::unlink(path) {
            Ok(()) => {
                let elapsed = start.elapsed();
                stats.record(elapsed, 0, get_iowait() - start_iowait);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

impl Operation for Unlink {
    fn get_stats(&self) -> Stats {
        self.stats.read().unwrap().clone()
    }
}

pub struct Rename {
    stats: RwLock<Stats>,
}

impl Rename {
    pub fn new() -> Rename {
        Rename {
            stats: RwLock::new(Stats::new()),
        }
    }

    pub fn run<P: AsRef<Path>, Q: AsRef<Path>>(&mut self, from_path: &P, to_path: &Q) -> io::Result<()> {
        let mut stats = self.stats.write().unwrap();
        let start_iowait = get_iowait();
        let start = Instant::now();
        match fs::rename(from_path, to_path) {
            Ok(()) => {
                let elapsed = start.elapsed();
                stats.record(elapsed, 0, get_iowait() - start_iowait);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

impl Operation for Rename {
    fn get_stats(&self) -> Stats {
        self.stats.read().unwrap().clone()
    }
}

pub struct ReadDir {
    stats: RwLock<Stats>,
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
        let start_iowait = get_iowait();
        let start = Instant::now();
        let mut bytes = 0;
        for entry in readdir {
            match entry {
                Ok(entry) => bytes += entry.file_name().len() + mem::size_of::<libc::dirent>() - 1,
                Err(e) => return Err(e),
            }
        }
        let elapsed = start.elapsed();
        stats.record(elapsed, 0, get_iowait() - start_iowait);
        Ok(())
    }
}

impl Operation for ReadDir {
    fn get_stats(&self) -> Stats {
        self.stats.read().unwrap().clone()
    }
}

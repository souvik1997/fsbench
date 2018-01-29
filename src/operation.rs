use super::nix;
use super::libc;
use super::util;
//use libc::c_char;
use std::mem;
//use std::ffi::OsStr;
//use std::os::unix::ffi::OsStrExt;
use nix::fcntl::OFlag;
use nix::sys::stat::Mode;
use std::os::unix::io::RawFd;
use std::time::{Duration, Instant};
use std::path::Path;
use std::fs;
use std::fmt;
use std::io;

pub struct Stats {
    num_ops: u32,
    total_latency_ns: Duration,
    total_bytes: usize,
}

impl Stats {
    pub fn new() -> Stats {
        Stats {
            num_ops: 0,
            total_latency_ns: Duration::new(0, 0),
            total_bytes: 0,
        }
    }
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let avg_latency = match self.total_latency_ns.checked_div(self.num_ops) {
            Some(quotient) => format!("{}.{:09}", quotient.as_secs(), quotient.subsec_nanos()),
            None => String::from("(inf)"),
        };
        write!(
            f,
            "Completed {} operations ({} bytes) in {}.{:09} s\n",
            self.num_ops,
            self.total_bytes,
            self.total_latency_ns.as_secs(),
            self.total_latency_ns.subsec_nanos()
        )?;
        write!(f, " - Average latency = {}\n", avg_latency)?;
        write!(
            f,
            " - Bytes/Operation = {}\n",
            (self.total_bytes as f64) / (self.num_ops as f64)
        )?;
        let ops_per_second = (self.num_ops as f64)
            / (self.total_latency_ns.as_secs() as f64
                + (self.total_latency_ns.subsec_nanos() as f64 / 1_000_000_000 as f64));
        write!(f, " - Operations/Second = {}\n", ops_per_second)?;
        Ok(())
    }
}

impl ::std::ops::Add for Stats {
    type Output = Stats;
    fn add(self, rhs: Stats) -> Self::Output {
        Stats {
            num_ops: self.num_ops + rhs.num_ops,
            total_latency_ns: self.total_latency_ns + rhs.total_latency_ns,
            total_bytes: self.total_bytes + rhs.total_bytes,
        }
    }
}

pub struct Open {
    pub stats: Stats,
}

impl Open {
    pub fn new() -> Open {
        Open {
            stats: Stats::new(),
        }
    }

    pub fn run<P: ?Sized + nix::NixPath>(
        &mut self,
        path: &P,
        oflag: OFlag,
        mode: Mode,
    ) -> nix::Result<RawFd> {
        let start = Instant::now();
        match nix::fcntl::open(path, oflag, mode) {
            Ok(fd) => {
                self.stats.total_latency_ns += start.elapsed();
                self.stats.num_ops += 1;
                Ok(fd)
            }
            Err(e) => Err(e),
        }
    }
}

pub struct Close {
    pub stats: Stats,
}

impl Close {
    pub fn new() -> Close {
        Close {
            stats: Stats::new(),
        }
    }

    pub fn run(&mut self, fd: RawFd) -> nix::Result<()> {
        let start = Instant::now();
        match nix::unistd::close(fd) {
            Ok(()) => {
                self.stats.total_latency_ns += start.elapsed();
                self.stats.num_ops += 1;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

pub struct Fsync {
    pub stats: Stats,
}

impl Fsync {
    pub fn new() -> Fsync {
        Fsync {
            stats: Stats::new(),
        }
    }

    pub fn run(&mut self, fd: RawFd) -> nix::Result<()> {
        let start = Instant::now();
        match nix::unistd::fsync(fd) {
            Ok(()) => {
                self.stats.total_latency_ns += start.elapsed();
                self.stats.num_ops += 1;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

pub struct Sync {
    pub stats: Stats,
}

impl Sync {
    pub fn new() -> Sync {
        Sync {
            stats: Stats::new(),
        }
    }

    pub fn run(&mut self) {
        let start = Instant::now();
        util::sync_all();
        self.stats.total_latency_ns += start.elapsed();
        self.stats.num_ops += 1;
    }
}

pub struct Read {
    pub stats: Stats,
}

impl Read {
    pub fn new() -> Read {
        Read {
            stats: Stats::new(),
        }
    }

    pub fn run(&mut self, fd: RawFd, buf: &mut [u8]) -> nix::Result<usize> {
        let start = Instant::now();
        match nix::unistd::read(fd, buf) {
            Ok(bytes_read) => {
                self.stats.total_latency_ns += start.elapsed();
                self.stats.num_ops += 1;
                self.stats.total_bytes += bytes_read;
                Ok(bytes_read)
            }
            Err(e) => Err(e),
        }
    }
}

pub struct Write {
    pub stats: Stats,
}

impl Write {
    pub fn new() -> Write {
        Write {
            stats: Stats::new(),
        }
    }

    pub fn run(&mut self, fd: RawFd, buf: &[u8]) -> nix::Result<usize> {
        let start = Instant::now();
        match nix::unistd::write(fd, buf) {
            Ok(bytes_written) => {
                self.stats.total_latency_ns += start.elapsed();
                self.stats.num_ops += 1;
                self.stats.total_bytes += bytes_written;
                Ok(bytes_written)
            }
            Err(e) => Err(e),
        }
    }
}

pub struct Unlink {
    pub stats: Stats,
}

impl Unlink {
    pub fn new() -> Unlink {
        Unlink {
            stats: Stats::new(),
        }
    }

    pub fn run<P: ?Sized + nix::NixPath>(&mut self, path: &P) -> nix::Result<()> {
        let start = Instant::now();
        match nix::unistd::unlink(path) {
            Ok(()) => {
                self.stats.total_latency_ns += start.elapsed();
                self.stats.num_ops += 1;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

pub struct Rename {
    pub stats: Stats,
}

impl Rename {
    pub fn new() -> Rename {
        Rename {
            stats: Stats::new(),
        }
    }

    pub fn run<P: AsRef<Path>, Q: AsRef<Path>>(
        &mut self,
        from_path: &P,
        to_path: &Q,
    ) -> io::Result<()> {
        let start = Instant::now();
        match fs::rename(from_path, to_path) {
            Ok(()) => {
                self.stats.total_latency_ns += start.elapsed();
                self.stats.num_ops += 1;
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

pub struct ReadDir {
    pub stats: Stats,
}

impl ReadDir {
    pub fn new() -> ReadDir {
        ReadDir {
            stats: Stats::new(),
        }
    }

    pub fn run<P: AsRef<Path> + ::std::fmt::Debug>(&mut self, path: P) -> io::Result<()> {
        let readdir = fs::read_dir(path)?;
        let start = Instant::now();
        let mut bytes = 0;
        for entry in readdir {
            match entry {
                Ok(entry) => bytes += entry.file_name().len() + mem::size_of::<libc::dirent>() - 1,
                Err(e) => return Err(e),
            }
        }
        self.stats.total_latency_ns += start.elapsed();
        self.stats.num_ops += 1;
        self.stats.total_bytes += bytes;
        Ok(())
        /*
        let ffi_path = path.as_ref().as_os_str().as_bytes();
        let dirname = ffi_path.as_ptr() as *const c_char;
        let mut bytes: usize = 0;
        let start = Instant::now();
        unsafe {
            let dir = libc::opendir(dirname);
            if dir.is_null() {
                println!("opendir failed on {:?}", path);
                return false;
            }
            loop {
                let dirent = libc::readdir(dir);
                if dirent.is_null() {
                    break;
                }
                bytes += libc::strlen((*dirent).d_name.as_ptr()) + mem::size_of::<libc::dirent>() - 1;
            }
            libc::closedir(dir);
        }
        self.stats.total_latency_ns += start.elapsed();
        self.stats.num_ops += 1;
        self.stats.total_bytes += bytes;
        true
        */
    }
}

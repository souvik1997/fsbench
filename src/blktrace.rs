use super::libc;
use super::nix;
use std::path::{Path, PathBuf};
use std::os::unix::io::RawFd;
use std::str;
use std::thread;
use std::thread::Thread;
use std::io;


mod blktrace_api {
    use super::*;
    use std::mem;


    /*
struct blk_user_trace_setup {
    char name[32];			// output
    __u16 act_mask;			// input
    __u32 buf_size;			// input
    __u32 buf_nr;       // input
    __u64 start_lba;
    __u64 end_lba;
    __u32 pid;
};
     */

    #[repr(C)]
    #[derive(Default)]
    pub struct BlkUserTraceSetup {
        pub name: [u8; 32],
        pub act_mask: u16,
        pub buf_size: u32,
        pub buf_nr: u32,
        pub start_lba: u64,
        pub end_lba: u64,
        pub pid: u32,
    }

    pub fn setup(fd: RawFd, obj: *mut BlkUserTraceSetup) -> i32 {
        use super::nix::*;
        const BLKTRACESETUP_MAGIC1: u8 = 0x12;
        const BLKTRACESETUP_MAGIC2: u8 = 115;
        unsafe {
            libc::ioctl(fd, iorw!(BLKTRACESETUP_MAGIC1, BLKTRACESETUP_MAGIC2, mem::size_of::<BlkUserTraceSetup>()), obj)
        }
    }

    pub fn start(fd: RawFd) -> i32 {
        use super::nix::*;
        const BLKTRACESTART_MAGIC1: u8 = 0x12;
        const BLKTRACESTART_MAGIC2: u8 = 116;
        unsafe {
            libc::ioctl(fd, io!(BLKTRACESTART_MAGIC1, BLKTRACESTART_MAGIC2))
        }
    }

    pub fn stop(fd: RawFd) -> i32 {
        use super::nix::*;
        const BLKTRACESTOP_MAGIC1: u8 = 0x12;
        const BLKTRACESTOP_MAGIC2: u8 = 117;
        unsafe {
            libc::ioctl(fd, io!(BLKTRACESTOP_MAGIC1, BLKTRACESTOP_MAGIC2))
        }
    }

    pub fn teardown(fd: RawFd) -> i32 {
        use super::nix::*;
        const BLKTRACETEARDOWN_MAGIC1: u8 = 0x12;
        const BLKTRACETEARDOWN_MAGIC2: u8 = 118;
        unsafe {
            libc::ioctl(fd, io!(BLKTRACETEARDOWN_MAGIC1, BLKTRACETEARDOWN_MAGIC2))
        }
    }
}

#[derive(Clone, Copy)]
pub struct BlktraceConfig {
    buffer_size: u32,
    buffer_subbuffers: u32,
    trace_mask: u16,
}

impl BlktraceConfig {
    pub fn set_trace_mask(&self, mask: u16) -> Self {
        let mut s = self.clone();
        s.trace_mask = mask;
        s
    }
    pub fn set_buffer_size(&self, buffer_size: u32) -> Self {
        let mut s = self.clone();
        s.buffer_size = buffer_size;
        s
    }
    pub fn set_buffer_subbuffers(&self, buffer_subbuffers: u32) -> Self {
        let mut s = self.clone();
        s.buffer_subbuffers = buffer_subbuffers;
        s
    }

    pub fn default() -> Self {
        return BlktraceConfig {
            buffer_size: 1024 * 512,
            buffer_subbuffers: 4,
            trace_mask: !0,
        }
    }
}

type Buffer = Vec<u8>;
pub struct Trace {
    pub data: Vec<Buffer>
}

impl Trace {
    pub fn new(data: Vec<Buffer>) -> Self {
        Self {
            data: data
        }
    }

    pub fn num_cpus(&self) -> usize {
        self.data.len()
    }

    pub fn total_bytes(&self) -> usize {
        self.data.iter().fold(0, |acc, s| acc + s.len())
    }

    pub fn export<P: AsRef<Path>, Q: AsRef<Path>>(&self, path: &P, prefix: &Q) -> io::Result<()> {
        use std::io::Write;
        use std::fs::File;
        use std::ops::Deref;
        use util::mkdir;
        mkdir(path.as_ref())?;
        for (index, buf) in self.data.iter().enumerate() {
            let mut filename = PathBuf::new();
            filename.set_file_name(prefix.as_ref());
            filename.set_extension(format!("blktrace.{}", index));
            let mut full_filename = PathBuf::new();
            full_filename.push(path);
            full_filename.push(filename);
            match File::create(full_filename) {
                Ok(mut file) => {
                    file.write_all(buf);
                },
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(())
    }
}

pub struct Blktrace {
    trace_paths: Vec<PathBuf>,
    device_path: PathBuf,
    device_name: String,
    blk_setup: blktrace_api::BlkUserTraceSetup,
    blktrace_fd: RawFd,
}

impl Blktrace {
    // Path should be a block device path, e.g. /dev/sda
    pub fn new<P: AsRef<Path>>(path: PathBuf, config: BlktraceConfig, debugfs_path: P) -> nix::Result<Self> {
        use blktrace::blktrace_api::BlkUserTraceSetup;
        use blktrace::blktrace_api::stop;
        use blktrace::blktrace_api::teardown;

        use blktrace::blktrace_api::setup;
        use blktrace::blktrace_api::start;
        use std::fs;
        use std::path::Component;

        let mut buts = BlkUserTraceSetup::default();
        buts.act_mask = config.trace_mask;
        buts.buf_nr = config.buffer_subbuffers;
        buts.buf_size = config.buffer_size;
        let fd = nix::fcntl::open(&path, nix::fcntl::OFlag::O_RDONLY | nix::fcntl::OFlag::O_NONBLOCK, nix::sys::stat::Mode::S_IRWXU)?;

        const MAX_TRIES: usize = 10;
        let mut tries: usize = 0;
        while setup(fd, &mut buts as *mut BlkUserTraceSetup) != 0 && tries < MAX_TRIES{
            stop(fd);
            teardown(fd);
            tries += 1;
        }
        if tries >= MAX_TRIES {
            println!("failed to start blktrace on {} after {} tries", fd, tries);
            return Err(nix::Error::last());
        }
        let device_name_bytes = buts.name.clone();
        let device_name_length = buts.name.iter().position(|c| *c == 0).unwrap_or(device_name_bytes.len());
        let device_name = str::from_utf8(&device_name_bytes[0..device_name_length]).expect("failed to parse device name as utf8");
        let start_result = start(fd);
        println!("blktrace start result on fd {}: {}", fd, start_result);
        let trace_directory = Path::new(debugfs_path.as_ref()).join("block").join(device_name);
        println!("scanning {:?}", trace_directory);
        let mut trace_paths: Vec<PathBuf> = fs::read_dir(trace_directory).expect("failed to read trace directory").filter_map(|path| {
            match path {
                Ok(ref readdir_entry) => {
                    let path = readdir_entry.path();
                    let path_components: Vec<Component> = path.components().collect();
                    let last_path_component: Option<&Component> = path_components.last();
                    match last_path_component {
                        Some(&Component::Normal(ref name)) => {
                            match name.to_str() {
                                Some(name_utf8) => {
                                    if name_utf8.len() < 6 {
                                        None
                                    } else {
                                        if name_utf8.starts_with("trace") {
                                            Some(path.to_owned())
                                        } else {
                                            None
                                        }
                                    }
                                },
                                None => None
                            }

                        },
                        _ => None
                    }
                }
                Err(_) => None,
            }
        }).collect();
        trace_paths.sort();
        Ok(Blktrace {
            trace_paths: trace_paths,
            device_path: path,
            blk_setup: buts,
            device_name: device_name.to_string(),
            blktrace_fd: fd,
        })
    }

    pub fn record_with<F: FnMut() -> ()>(&self, mut task: F) -> nix::Result<Trace> {
        use nix::poll::PollFd;
        use nix::poll::EventFlags;
        use nix::poll::poll;
        use std::time::Duration;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use std::sync::RwLock;
        use std::os::unix::io::FromRawFd;
        use std::fs::File;
        use std::io::Read;
        use util::drop_cache;

        let buffers: Arc<RwLock<Vec<Buffer>>> = {
            let mut v = Vec::new();
            v.resize(self.trace_paths.len(), Vec::new());
            Arc::new(RwLock::new(v))
        };
        println!("buffer size: {}", buffers.read().unwrap().len());
        let mut file_descriptors: Vec<RawFd> = Vec::new();
        for path in &self.trace_paths {
            file_descriptors.push(nix::fcntl::open(path, nix::fcntl::OFlag::O_RDONLY | nix::fcntl::OFlag::O_NONBLOCK, nix::sys::stat::Mode::S_IRWXU)?);
        }
        let cancel_flag = Arc::new(AtomicBool::new(false));
        let cancel_flag_thread = cancel_flag.clone();
        let buffers_thread = buffers.clone();
        let thread = thread::spawn(move || {
            let mut files: Vec<File> = file_descriptors.iter().map(|fd| unsafe { File::from_raw_fd(*fd) }).collect();
            let mut poll_file_descriptors: Vec<PollFd> = file_descriptors.iter().map(|fd| PollFd::new(*fd, EventFlags::POLLIN)).collect();
            while !cancel_flag_thread.load(Ordering::SeqCst) {
                match poll(&mut poll_file_descriptors, 500) {
                    Ok(_) => {
                        for (index, pfd) in poll_file_descriptors.iter().enumerate() {
                            if pfd.revents().expect("failed to get revents from poll fd").contains(EventFlags::POLLIN) {
                                files[index].read_to_end(&mut buffers_thread.write().unwrap()[index]).expect("failed to read from trace file");
                            }
                        }
                    },
                    Err(_) => {
                        return;
                    }
                }
            }
        });
        drop_cache();
        thread::sleep(Duration::from_millis(10000));
        task();
        drop_cache();
        thread::sleep(Duration::from_millis(10000));
        cancel_flag.store(true, Ordering::SeqCst);
        thread.join().expect("failed to join thread");
        Ok(Trace::new(Arc::try_unwrap(buffers).expect("failed to unwrap buffers from Arc<>").into_inner().expect("failed to get data out of rwlock")))
    }
}


impl Drop for Blktrace {
    fn drop(&mut self) {
        use blktrace::blktrace_api::stop;
        use blktrace::blktrace_api::teardown;
        stop(self.blktrace_fd);
        teardown(self.blktrace_fd);
    }
}

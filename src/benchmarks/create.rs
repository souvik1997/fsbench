use super::BaseConfiguration;
use super::Benchmark;
use super::Config;
use super::fsbench::blktrace::*;
use super::fsbench::fileset::*;
use super::fsbench::operation::*;
use super::fsbench::statistics::*;
use super::fsbench::util::*;
use super::nix;
use super::serde_json;
use std::error::Error;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Serialize, Deserialize)]
pub struct CreateFilesConfig {
    num_files: usize,
    dir_width: usize,
}

impl CreateFilesConfig {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, Box<Error>> {
        use std::fs::File;
        let file = File::open(path)?;
        let c = serde_json::from_reader(file)?;
        Ok(c)
    }
}


const EXT4_DIR_WIDTH: usize = 7;
const EXT4_NUM_FILES: usize = 30000;

impl Config for CreateFilesConfig {
    fn config_for(fs: &Filesystem) -> Self {
        match fs {
            &Filesystem::Ext4 | &Filesystem::Ext4NoJournal => {
                Self {
                    num_files: EXT4_NUM_FILES,
                    dir_width: EXT4_DIR_WIDTH,
                }
            },
            _ => {
                Self {
                    num_files: super::DEFAULT_NUM_FILES,
                    dir_width: super::DEFAULT_DIR_WIDTH,
                }
            }
        }
    }

    fn num_files(&self) -> usize { self.num_files }
}

#[derive(Serialize, Deserialize)]
pub struct CreateFilesBatchSyncConfig {
    num_files: usize,
    dir_width: usize,
    batch_size: usize,
}

impl CreateFilesBatchSyncConfig {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, Box<Error>> {
        use std::fs::File;
        let file = File::open(path)?;
        let c = serde_json::from_reader(file)?;
        Ok(c)
    }
}

impl Config for CreateFilesBatchSyncConfig {
    fn config_for(fs: &Filesystem) -> Self {
        match fs {
            _ => {
                Self {
                    num_files: super::DEFAULT_NUM_FILES,
                    dir_width: super::DEFAULT_DIR_WIDTH,
                    batch_size: 10,
                }
            }
        }
    }

    fn num_files(&self) -> usize { self.num_files }
}

#[derive(Serialize, Deserialize)]
pub struct CreateFilesEachSyncConfig {
    num_files: usize,
    dir_width: usize,
}

impl CreateFilesEachSyncConfig {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, Box<Error>> {
        use std::fs::File;
        let file = File::open(path)?;
        let c = serde_json::from_reader(file)?;
        Ok(c)
    }
}

impl Config for CreateFilesEachSyncConfig {
    fn config_for(fs: &Filesystem) -> Self {
        match fs {
            _ => {
                Self {
                    num_files: super::DEFAULT_NUM_FILES,
                    dir_width: super::DEFAULT_DIR_WIDTH,
                }
            }
        }
    }

    fn num_files(&self) -> usize { self.num_files }
}

pub struct CreateFiles<'a> {
    data: CreateFilesShared,
    base_config: &'a BaseConfiguration<'a>,
    createfiles_config: &'a CreateFilesConfig,
}

pub struct CreateFilesBatchSync<'a> {
    data: CreateFilesShared,
    base_config: &'a BaseConfiguration<'a>,
    createfiles_config: &'a CreateFilesBatchSyncConfig,
}

pub struct CreateFilesEachSync<'a> {
    data: CreateFilesShared,
    base_config: &'a BaseConfiguration<'a>,
    createfiles_config: &'a CreateFilesEachSyncConfig,
}

impl<'a> CreateFiles<'a> {
    pub fn run(base_config: &'a BaseConfiguration, createfiles_config: &'a CreateFilesConfig) -> Self {
        let base_path = base_config.filesystem_path.join("createfiles");
        Self {
            data: CreateFilesShared::run(
                FileSet::new(createfiles_config.num_files, &base_path, createfiles_config.dir_width),
                &base_config.blktrace,
                None,
            ),
            base_config: base_config,
            createfiles_config: createfiles_config,
        }
    }

    pub fn export(&self) -> io::Result<()> {
        use std::fs::File;
        let path = self.base_config.output_dir.join("createfiles");
        mkdir(&path)?;
        serde_json::to_writer(File::create(path.join("config.json"))?, &self.createfiles_config)?;
        self.data.export(path)
    }
}

impl<'a> Benchmark<CreateFilesConfig> for CreateFiles<'a> {
    fn total(&self) -> Stats {
        self.data.total()
    }

    fn get_trace<'b>(&'b self) -> &'b Trace {
        &self.data.trace
    }
    fn get_config<'b>(&'b self) -> &'b CreateFilesConfig {
        &self.createfiles_config
    }
}

impl<'a> CreateFilesBatchSync<'a> {
    pub fn run(base_config: &'a BaseConfiguration, createfiles_config: &'a CreateFilesBatchSyncConfig) -> Self {
        let base_path = base_config.filesystem_path.join("createfiles_batchsync");
        Self {
            data: CreateFilesShared::run(
                FileSet::new(createfiles_config.num_files, &base_path, createfiles_config.dir_width),
                &base_config.blktrace,
                Some(createfiles_config.batch_size),
            ),
            base_config: base_config,
            createfiles_config: createfiles_config,
        }
    }

    pub fn export(&self) -> io::Result<()> {
        use std::fs::File;
        let path = self.base_config.output_dir.join("createfiles_batchsync");
        mkdir(&path)?;
        serde_json::to_writer(File::create(path.join("config.json"))?, &self.createfiles_config)?;
        self.data.export(path)
    }
}

impl<'a> Benchmark<CreateFilesBatchSyncConfig> for CreateFilesBatchSync<'a> {
    fn total(&self) -> Stats {
        self.data.total()
    }

    fn get_trace<'b>(&'b self) -> &'b Trace {
        &self.data.trace
    }

    fn get_config<'b>(&'b self) -> &'b CreateFilesBatchSyncConfig {
        &self.createfiles_config
    }
}

impl<'a> CreateFilesEachSync<'a> {
    pub fn run(base_config: &'a BaseConfiguration, createfiles_config: &'a CreateFilesEachSyncConfig) -> Self {
        let base_path = base_config.filesystem_path.join("createfiles_eachsync");
        Self {
            data: CreateFilesShared::run(
                FileSet::new(createfiles_config.num_files, &base_path, createfiles_config.dir_width),
                &base_config.blktrace,
                Some(0),
            ),
            base_config: base_config,
            createfiles_config: createfiles_config,
        }
    }

    pub fn export(&self) -> io::Result<()> {
        use std::fs::File;
        let path = self.base_config.output_dir.join("createfiles_eachsync");
        mkdir(&path)?;
        serde_json::to_writer(File::create(path.join("config.json"))?, &self.createfiles_config)?;
        self.data.export(path)
    }
}

impl<'a> Benchmark<CreateFilesEachSyncConfig> for CreateFilesEachSync<'a> {
    fn total(&self) -> Stats {
        self.data.total()
    }

    fn get_trace<'b>(&'b self) -> &'b Trace {
        &self.data.trace
    }

    fn get_config<'b>(&'b self) -> &'b CreateFilesEachSyncConfig {
        &self.createfiles_config
    }
}

struct CreateFilesShared {
    open: Stats,
    close: Stats,
    fsync: Stats,
    sync: Stats,
    trace: Trace,
}

impl CreateFilesShared {
    pub fn run(file_set: FileSet, blktrace: &Blktrace, batch_size: Option<usize>) -> Self {
        use super::rand;
        use rand::Rng;
        use std::os::unix::io::RawFd;

        drop_cache();
        let file_set: Vec<PathBuf> = {
            let mut f: Vec<PathBuf> = file_set.into_iter().collect();
            rand::thread_rng().shuffle(&mut f);
            f
        };
        let mut open = Open::new();
        let mut close = Close::new();
        let mut fsync = Fsync::new();
        let mut sync = Sync::new();

        for file in &file_set {
            let parent_path = file.parent().expect("file should have parent");
            mkdir(parent_path).expect("failed to construct directory tree");
        }

        let trace = blktrace
            .record_with(|| {
                // Create directory structure and files
                let mut fd_queue: Vec<(RawFd, &Path)> = Vec::new();
                fd_queue.reserve(batch_size.unwrap_or(0));
                for file in &file_set {
                    let parent_path = file.parent().expect("file should have parent");
                    assert!(parent_path.is_dir());
                    let fd = open.run(
                        file,
                        nix::fcntl::OFlag::O_CREAT | nix::fcntl::OFlag::O_RDWR,
                        nix::sys::stat::Mode::S_IRWXU,
                    ).expect("failed to create file");

                    if let Some(batch_size) = batch_size {
                        if fd_queue.len() >= batch_size {
                            for &(ifd, containing_directory) in &fd_queue {
                                fsync.run(ifd).expect("failed to fsync file");
                                close.run(ifd).expect("failed to close file");
                                let dir_fd =
                                    nix::fcntl::open(containing_directory, nix::fcntl::OFlag::O_DIRECTORY, nix::sys::stat::Mode::S_IRWXU)
                                        .expect("failed to open parent directory");
                                nix::unistd::fsync(dir_fd).expect("failed to fsync parent directory");
                                nix::unistd::close(dir_fd).expect("failed to close dir fd");
                            }
                            fd_queue.clear();
                            fd_queue.reserve(batch_size);
                        }
                        fd_queue.push((fd, parent_path));
                    } else {
                        close.run(fd).expect("failed to close file");
                    }
                }
                for &(ifd, containing_directory) in &fd_queue {
                    fsync.run(ifd).expect("failed to fsync file");
                    close.run(ifd).expect("failed to close file");
                    let dir_fd = nix::fcntl::open(containing_directory, nix::fcntl::OFlag::O_DIRECTORY, nix::sys::stat::Mode::S_IRWXU)
                        .expect("failed to open parent directory");
                    nix::unistd::fsync(dir_fd).expect("failed to fsync parent directory");
                    nix::unistd::close(dir_fd).expect("failed to close dir fd");
                }
                sync.run();
            })
            .expect("failed to record trace");

        info!("Finished micro-create:");
        let open_stats = open.get_stats();
        let close_stats = close.get_stats();
        let fsync_stats = fsync.get_stats();
        let sync_stats = sync.get_stats();
        info!(" - Open: {}", open_stats);
        info!(" - Close: {}", close_stats);
        info!(" - Fsync: {}", fsync_stats);
        info!(" - Sync: {}", sync_stats);
        info!(
            " - Total: {}",
            open_stats.clone() + close_stats.clone() + fsync_stats.clone() + sync_stats.clone()
        );
        info!(" - Blktrace recorded {} bytes on {} cpus", trace.total_bytes(), trace.num_cpus());
        drop_cache();
        Self {
            open: open_stats,
            close: close_stats,
            fsync: fsync_stats,
            sync: sync_stats,
            trace: trace,
        }
    }

    fn export<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        use std::fs::File;
        serde_json::to_writer(File::create(path.as_ref().join("open.json"))?, &self.open)?;
        serde_json::to_writer(File::create(path.as_ref().join("close.json"))?, &self.close)?;
        serde_json::to_writer(File::create(path.as_ref().join("fsync.json"))?, &self.fsync)?;
        serde_json::to_writer(File::create(path.as_ref().join("sync.json"))?, &self.sync)?;
        self.trace.export(&path, &"blktrace")
    }

    fn total(&self) -> Stats {
        self.open.clone() + self.close.clone() + self.fsync.clone() + self.sync.clone()
    }
}

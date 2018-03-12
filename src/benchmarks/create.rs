use super::fsbench::operation::*;
use super::fsbench::statistics::*;
use super::fsbench::blktrace::*;
use super::fsbench::util::*;
use super::fsbench::fileset::*;
use super::nix;
use super::BaseConfiguration;
use super::serde_json;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::io;

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

impl Default for CreateFilesConfig {
    fn default() -> Self {
        Self {
            num_files: super::DEFAULT_NUM_FILES,
            dir_width: super::DEFAULT_DIR_WIDTH,
        }
    }
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

impl Default for CreateFilesBatchSyncConfig {
    fn default() -> Self {
        Self {
            num_files: super::DEFAULT_NUM_FILES,
            dir_width: super::DEFAULT_DIR_WIDTH,
            batch_size: 10,
        }
    }
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

impl Default for CreateFilesEachSyncConfig {
    fn default() -> Self {
        Self {
            num_files: super::DEFAULT_NUM_FILES,
            dir_width: super::DEFAULT_DIR_WIDTH,
        }
    }
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
                FileSet::new(
                    createfiles_config.num_files,
                    &base_path,
                    createfiles_config.dir_width,
                ),
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
        serde_json::to_writer(
            File::create(path.join("config.json"))?,
            &self.createfiles_config,
        )?;
        self.data.export(path)
    }
}

impl<'a> CreateFilesBatchSync<'a> {
    pub fn run(base_config: &'a BaseConfiguration, createfiles_config: &'a CreateFilesBatchSyncConfig) -> Self {
        let base_path = base_config.filesystem_path.join("createfiles_batchsync");
        Self {
            data: CreateFilesShared::run(
                FileSet::new(
                    createfiles_config.num_files,
                    &base_path,
                    createfiles_config.dir_width,
                ),
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
        serde_json::to_writer(
            File::create(path.join("config.json"))?,
            &self.createfiles_config,
        )?;
        self.data.export(path)
    }
}

impl<'a> CreateFilesEachSync<'a> {
    pub fn run(base_config: &'a BaseConfiguration, createfiles_config: &'a CreateFilesEachSyncConfig) -> Self {
        let base_path = base_config.filesystem_path.join("createfiles_eachsync");
        Self {
            data: CreateFilesShared::run(
                FileSet::new(
                    createfiles_config.num_files,
                    &base_path,
                    createfiles_config.dir_width,
                ),
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
        serde_json::to_writer(
            File::create(path.join("config.json"))?,
            &self.createfiles_config,
        )?;
        self.data.export(path)
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
        drop_cache();
        let file_set: Vec<PathBuf> = file_set.into_iter().collect();
        let mut open = Open::new();
        let mut close = Close::new();
        let mut fsync = Fsync::new();
        let mut sync = Sync::new();

        let trace = blktrace
            .record_with(|| {
                // Create directory structure and files
                let mut fd_queue = Vec::new();
                fd_queue.reserve(batch_size.unwrap_or(0));
                for file in &file_set {
                    if let Some(parent_path) = file.parent() {
                        mkdir(parent_path).expect("failed to construct directory tree");
                        assert!(parent_path.is_dir());
                        let fd = open.run(
                            file,
                            nix::fcntl::OFlag::O_CREAT | nix::fcntl::OFlag::O_RDWR,
                            nix::sys::stat::Mode::S_IRWXU,
                        ).expect("failed to create file");

                        if let Some(batch_size) = batch_size {
                            if fd_queue.len() >= batch_size {
                                for ifd in &fd_queue {
                                    fsync.run(*ifd).expect("failed to fsync file");
                                    close.run(*ifd).expect("failed to close file");
                                }
                                fd_queue.clear();
                                fd_queue.reserve(batch_size);
                            }
                            fd_queue.push(fd);
                        } else {
                            close.run(fd).expect("failed to close file");
                        }
                    }
                }
                for ifd in &fd_queue {
                    fsync.run(*ifd).expect("failed to fsync file");
                    close.run(*ifd).expect("failed to close file");
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
        info!(
            " - Blktrace recorded {} bytes on {} cpus",
            trace.total_bytes(),
            trace.num_cpus()
        );
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
}
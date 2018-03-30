use super::BaseConfiguration;
use super::Benchmark;
use super::fsbench::blktrace::*;
use super::fsbench::fileset::*;
use super::fsbench::operation::*;
use super::fsbench::statistics::*;
use super::fsbench::util::*;
use super::nix;
use super::serde_json;
use std::io;

use std::path::{Path, PathBuf};

pub struct DeleteFiles<'a> {
    open: Stats,
    close: Stats,
    unlink: Stats,
    trace: Trace,
    base_config: &'a BaseConfiguration<'a>,
    deletefiles_config: &'a DeleteFilesConfig,
}

#[derive(Serialize, Deserialize)]
pub struct DeleteFilesConfig {
    num_files: usize,
    dir_width: usize,
}

use std::error::Error;

impl DeleteFilesConfig {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, Box<Error>> {
        use super::serde_json;
        use std::fs::File;
        let file = File::open(path)?;
        let c = serde_json::from_reader(file)?;
        Ok(c)
    }
}

impl Default for DeleteFilesConfig {
    fn default() -> Self {
        Self {
            num_files: super::DEFAULT_NUM_FILES,
            dir_width: super::DEFAULT_DIR_WIDTH,
        }
    }
}

impl<'a> DeleteFiles<'a> {
    pub fn run(base_config: &'a BaseConfiguration, config: &'a DeleteFilesConfig) -> Self {
        use super::rand;
        use rand::Rng;

        drop_cache();
        let config_path: &Path = base_config.filesystem_path.as_ref();
        let base_path = PathBuf::from(config_path.join("delete"));
        let file_set: Vec<PathBuf> = FileSet::new(config.num_files, &base_path, config.dir_width).into_iter().collect();
        let mut file_set_shuffled = file_set.clone();
        rand::thread_rng().shuffle(&mut file_set_shuffled);
        let mut open = Open::new();
        let mut close = Close::new();
        let mut unlink = Unlink::new();

        for file in file_set {
            if let Some(parent_path) = file.parent() {
                mkdir(parent_path).expect("failed to construct directory tree");
                assert!(parent_path.is_dir());
                let fd = open.run(&file, nix::fcntl::OFlag::O_CREAT, nix::sys::stat::Mode::S_IRWXU)
                    .expect("failed to create file");
                close.run(fd).expect("failed to close file");
            }
        }

        drop_cache();

        let trace = base_config
            .blktrace
            .record_with(|| {
                for file in &file_set_shuffled {
                    unlink.run(file).expect("failed to unlink file");
                }
            })
            .expect("failed to record trace");

        info!("Finished micro-delete:");
        let open_stats = open.get_stats();
        let close_stats = close.get_stats();
        let unlink_stats = unlink.get_stats();
        info!(" - Open: {}", open_stats);
        info!(" - Close: {}", close_stats);
        info!(" - Unlink: {}", unlink_stats);
        info!(" - Total: {}", open_stats.clone() + close_stats.clone() + unlink_stats.clone());
        info!(" - Blktrace recorded {} bytes on {} cpus", trace.total_bytes(), trace.num_cpus());
        drop_cache();
        Self {
            open: open_stats,
            close: close_stats,
            unlink: unlink_stats,
            trace: trace,
            base_config: base_config,
            deletefiles_config: config,
        }
    }

    pub fn export(&self) -> io::Result<()> {
        let path = self.base_config.output_dir.join("deletefiles");
        use std::fs::File;
        mkdir(&path)?;
        serde_json::to_writer(File::create(path.join("open.json"))?, &self.open)?;
        serde_json::to_writer(File::create(path.join("close.json"))?, &self.close)?;
        serde_json::to_writer(File::create(path.join("unlink.json"))?, &self.unlink)?;
        serde_json::to_writer(File::create(path.join("config.json"))?, &self.deletefiles_config)?;
        self.trace.export(&path, &"blktrace")
    }
}

impl<'a> Benchmark for DeleteFiles<'a> {
    fn total(&self) -> Stats {
        self.open.clone() + self.close.clone() + self.unlink.clone()
    }

    fn get_trace<'b>(&'b self) -> &'b Trace {
        &self.trace
    }
}

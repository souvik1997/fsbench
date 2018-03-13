use super::fsbench::operation::*;
use super::fsbench::statistics::*;
use super::fsbench::blktrace::*;
use super::fsbench::util::*;
use super::fsbench::fileset::*;
use super::nix;
use super::BaseConfiguration;
use super::serde_json;
use super::rand;
use std::path::{Path, PathBuf};
use rand::Rng;
use std::io;

pub struct RenameFiles<'a> {
    open: Stats,
    close: Stats,
    rename: Stats,
    trace: Trace,
    base_config: &'a BaseConfiguration<'a>,
    renamefiles_config: &'a RenameFilesConfig,
}

#[derive(Serialize, Deserialize)]
pub struct RenameFilesConfig {
    num_files: usize,
    dir_width: usize,
}

use std::error::Error;

impl RenameFilesConfig {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, Box<Error>> {
        use super::serde_json;
        use std::fs::File;
        let file = File::open(path)?;
        let c = serde_json::from_reader(file)?;
        Ok(c)
    }
}

impl Default for RenameFilesConfig {
    fn default() -> Self {
        Self {
            num_files: super::DEFAULT_NUM_FILES,
            dir_width: super::DEFAULT_DIR_WIDTH,
        }
    }
}

impl<'a> RenameFiles<'a> {
    pub fn run(base_config: &'a BaseConfiguration, config: &'a RenameFilesConfig) -> Self {
        drop_cache();
        let config_path: &Path = base_config.filesystem_path.as_ref();
        let base_path = PathBuf::from(config_path.join("rename"));
        let file_set: Vec<PathBuf> = FileSet::new(config.num_files, &base_path, config.dir_width)
            .into_iter()
            .collect();
        let mut file_set_shuffled = file_set.clone();
        rand::thread_rng().shuffle(&mut file_set_shuffled);
        let mut open = Open::new();
        let mut close = Close::new();
        let mut rename = Rename::new();

        for file in file_set {
            if let Some(parent_path) = file.parent() {
                mkdir(parent_path).expect("failed to construct directory tree");
                assert!(parent_path.is_dir());
                let fd = open.run(
                    &file,
                    nix::fcntl::OFlag::O_CREAT,
                    nix::sys::stat::Mode::S_IRWXU,
                ).expect("failed to create file");
                close.run(fd).expect("failed to close file");
            }
        }

        drop_cache();
        let trace = base_config
            .blktrace
            .record_with(|| {
                for file in &file_set_shuffled {
                    // Rename /path/to/file to /path/to/file.data
                    let new_path = file.with_extension("data");
                    rename.run(file, &new_path).expect("failed to rename file");
                }
            })
            .expect("failed to record trace");

        info!("Finished micro-rename:");
        let open_stats = open.get_stats();
        let close_stats = close.get_stats();
        let rename_stats = rename.get_stats();
        info!(" - Open: {}", open_stats);
        info!(" - Close: {}", close_stats);
        info!(" - Rename: {}", rename_stats);
        info!(
            " - Total: {}",
            open_stats.clone() + close_stats.clone() + rename_stats.clone()
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
            rename: rename_stats,
            trace: trace,
            base_config: base_config,
            renamefiles_config: config,
        }
    }

    pub fn export(&self) -> io::Result<()> {
        let path = self.base_config.output_dir.join("renamefiles");
        use std::fs::File;
        mkdir(&path)?;
        serde_json::to_writer(File::create(path.join("open.json"))?, &self.open)?;
        serde_json::to_writer(File::create(path.join("close.json"))?, &self.close)?;
        serde_json::to_writer(File::create(path.join("rename.json"))?, &self.rename)?;
        serde_json::to_writer(
            File::create(path.join("config.json"))?,
            &self.renamefiles_config,
        )?;
        self.trace.export(&path, &"blktrace")
    }

    pub fn total(&self) -> Stats {
        self.open.clone() + self.close.clone() + self.rename.clone()
    }
}

use super::BaseConfiguration;
use super::Benchmark;
use super::Config;
use super::fsbench::blktrace::*;
use super::fsbench::fileset::*;
use super::fsbench::operation::*;
use super::fsbench::statistics::*;
use super::fsbench::util::*;
use super::nix;
use super::rand;
use super::serde_json;
use rand::Rng;
use std::io;
use std::path::{Path, PathBuf};

pub struct ListDir<'a> {
    open: Stats,
    close: Stats,
    readdir: Stats,
    trace: Trace,
    base_config: &'a BaseConfiguration<'a>,
    listdir_config: &'a ListDirConfig,
}

#[derive(Serialize, Deserialize)]
pub struct ListDirConfig {
    num_files: usize,
    dir_width: usize,
    module_name: String,
}

use std::error::Error;

impl ListDirConfig {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, Box<Error>> {
        use super::serde_json;
        use std::fs::File;
        let file = File::open(path)?;
        let c = serde_json::from_reader(file)?;
        Ok(c)
    }
}

impl Config for ListDirConfig {
    fn config_for(fs: &Filesystem) -> Self {
        Self {
            num_files: super::DEFAULT_NUM_FILES,
            dir_width: super::DEFAULT_DIR_WIDTH,
            module_name: fs.module_name(),
        }
    }

    fn num_files(&self) -> usize { self.num_files }
}

impl<'a> ListDir<'a> {
    pub fn run(base_config: &'a BaseConfiguration, config: &'a ListDirConfig) -> Self {
        drop_cache();
        let config_path: &Path = base_config.filesystem_path.as_ref();
        let base_path = PathBuf::from(config_path.join("delete"));
        let file_set: Vec<PathBuf> = FileSet::new(config.num_files, &base_path, config.dir_width).into_iter().collect();
        let mut directories = Vec::<PathBuf>::new();
        let mut file_set_shuffled = file_set.clone();
        rand::thread_rng().shuffle(&mut file_set_shuffled);
        let mut open = Open::new();
        let mut close = Close::new();
        let mut readdir = ReadDir::new();

        for file in file_set {
            if let Some(parent_path) = file.parent() {
                mkdir(parent_path).expect("failed to construct directory tree");
                assert!(parent_path.is_dir());
                directories.push(parent_path.to_owned());
                let fd = open.run(&file, nix::fcntl::OFlag::O_CREAT, nix::sys::stat::Mode::S_IRWXU)
                    .expect("failed to create file");
                close.run(fd).expect("failed to close file");
            }
        }

        drop_cache();

        let trace = base_config
            .blktrace
            .record_with(|| {
                const ITERATIONS: usize = 1000000;
                for _ in 0..ITERATIONS {
                    let directory = rand::thread_rng()
                        .choose(&directories)
                        .expect("failed to randomly select directory");
                    readdir.run(directory).expect("failed to read directory");
                }
            })
            .expect("failed to record trace");

        let open_stats = open.get_stats();
        let close_stats = close.get_stats();
        let readdir_stats = readdir.get_stats();
        info!(" - Open: {}", open_stats);
        info!(" - Close: {}", close_stats);
        info!(" - Readdir: {}", readdir_stats);
        info!(" - Total: {}", open_stats.clone() + close_stats.clone() + readdir_stats.clone());
        info!(" - Blktrace recorded {} bytes on {} cpus", trace.total_bytes(), trace.num_cpus());
        drop_cache();
        Self {
            open: open_stats,
            close: close_stats,
            readdir: readdir_stats,
            trace: trace,
            base_config: base_config,
            listdir_config: config,
        }
    }

    pub fn export(&self) -> io::Result<()> {
        let path = self.base_config.output_dir.join("listdir");
        use std::fs::File;
        mkdir(&path)?;
        serde_json::to_writer(File::create(path.join("open.json"))?, &self.open)?;
        serde_json::to_writer(File::create(path.join("close.json"))?, &self.close)?;
        serde_json::to_writer(File::create(path.join("readdir.json"))?, &self.readdir)?;
        serde_json::to_writer(File::create(path.join("config.json"))?, &self.listdir_config)?;
        self.trace.export(&path, &"blktrace")
    }
}

impl<'a> Benchmark<ListDirConfig> for ListDir<'a> {
    fn total(&self) -> Stats {
        self.open.clone() + self.close.clone() + self.readdir.clone()
    }

    fn get_trace<'b>(&'b self) -> &'b Trace {
        &self.trace
    }

    fn get_config<'b>(&'b self) -> &'b ListDirConfig {
        &self.listdir_config
    }
}

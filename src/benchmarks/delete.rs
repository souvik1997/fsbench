use super::fsbench::operation::*;
use super::fsbench::statistics::*;
use super::fsbench::blktrace::*;
use super::fsbench::util::*;
use super::fsbench::fileset::*;
use super::nix;
use super::Configuration;
use super::rand;
use std::path::{Path, PathBuf};
use rand::Rng;
use rand::distributions::IndependentSample;

#[allow(dead_code)]
pub struct DeleteFiles {
    open: Stats,
    close: Stats,
    unlink: Stats,
    trace: Trace,
}

impl DeleteFiles {
    pub fn run<R: IndependentSample<f64>, RV: IndependentSample<f64>>(config: &Configuration<R, RV>) -> Self {
        drop_cache();
        let config_path: &Path = config.filesystem_path.as_ref();
        let base_path = PathBuf::from(config_path.join("delete"));
        let file_set: Vec<PathBuf> = FileSet::new(config.num_files, &base_path, config.dir_width)
            .into_iter()
            .collect();
        let mut file_set_shuffled = file_set.clone();
        rand::thread_rng().shuffle(&mut file_set_shuffled);
        let mut open = Open::new();
        let mut close = Close::new();
        let mut unlink = Unlink::new();

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

        let trace = config
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
        info!(
            " - Total: {}",
            open_stats.clone() + close_stats.clone() + unlink_stats.clone()
        );
        info!(
            " - Blktrace recorded {} bytes on {} cpus",
            trace.total_bytes(),
            trace.num_cpus()
        );
        drop_cache();
        trace.export(&config.output_dir, &"deletefiles");
        Self {
            open: open_stats,
            close: close_stats,
            unlink: unlink_stats,
            trace: trace,
        }
    }
}

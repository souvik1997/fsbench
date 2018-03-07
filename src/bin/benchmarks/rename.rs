use super::*;
use std::path::{Path, PathBuf};
use rand::Rng;
use rand::distributions::IndependentSample;
use std::ops::Deref;

#[allow(dead_code)]
pub struct RenameFiles {
    open: Stats,
    close: Stats,
    rename: Stats,
    trace: Trace,
}

impl RenameFiles {
    pub fn run<R: IndependentSample<f64>, RV: IndependentSample<f64>>(config: &Configuration<R, RV>) -> Self {
        drop_cache();
        let config_path: &Path = config.filesystem_path.as_ref();
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
        let trace = config
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
        let open_stats = open.stats.read().unwrap().deref().clone();
        let close_stats = close.stats.read().unwrap().deref().clone();
        let rename_stats = rename.stats.read().unwrap().deref().clone();
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
        trace.export(&config.output_dir, &"renamefiles");
        Self {
            open: open_stats,
            close: close_stats,
            rename: rename_stats,
            trace: trace,
        }
    }
}

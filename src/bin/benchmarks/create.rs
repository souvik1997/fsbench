use super::*;
use std::path::{Path, PathBuf};
use rand::distributions::IndependentSample;
use std::ops::Deref;
use std::marker;

#[allow(dead_code)]
pub struct CreateFiles {
    open: Stats,
    close: Stats,
    fsync: Stats,
    sync: Stats,
    trace: Trace,
}

impl CreateFiles {
    pub fn run<
        N: AsRef<Path>,
        R: IndependentSample<f64> + marker::Sync,
        RV: IndependentSample<f64> + marker::Sync,
    >(
        config: &Configuration<R, RV>,
        name: &N,
        batch_size: Option<usize>,
    ) -> Self {
        drop_cache();
        let config_path: &Path = config.filesystem_path.as_ref();
        let base_path = PathBuf::from(config_path.join(name));
        let file_set: Vec<PathBuf> = FileSet::new(config.num_files, &base_path, config.dir_width)
            .into_iter()
            .collect();
        let mut open = Open::new();
        let mut close = Close::new();
        let mut fsync = Fsync::new();
        let mut sync = Sync::new();

        let mut rng = rand::StdRng::new().unwrap();

        let trace = config
            .blktrace
            .record_with(|| {
                // Create directory structure and files
                let mut fd_queue = Vec::new();
                fd_queue.reserve(batch_size.unwrap_or(0));
                for (index, file) in file_set.iter().enumerate() {
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
        let open_stats = open.stats.read().unwrap().deref().clone();
        let close_stats = close.stats.read().unwrap().deref().clone();
        let fsync_stats = fsync.stats.read().unwrap().deref().clone();
        let sync_stats = sync.stats.read().unwrap().deref().clone();
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
        trace.export(&config.output_dir, name);
        Self {
            open: open_stats,
            close: close_stats,
            fsync: fsync_stats,
            sync: sync_stats,
            trace: trace,
        }
    }
}

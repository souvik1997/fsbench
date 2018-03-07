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
    write: Stats,
    trace: Trace,
}

impl CreateFiles {
    pub fn run<
        N: AsRef<Path>,
        F: Fn(usize) -> bool + marker::Sync,
        R: IndependentSample<f64> + marker::Sync,
        RV: IndependentSample<f64> + marker::Sync,
    >(
        config: &Configuration<R, RV>,
        name: &N,
        maybe_fsync: F,
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
        let mut write = Write::new();

        let mut write_buffer: Vec<u8> = Vec::new();
        let mut rng = rand::StdRng::new().unwrap();

        let trace = config
            .blktrace
            .record_with(|| {
                let thread_pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(config.num_threads)
                    .build()
                    .unwrap();
                // Create directory structure and files
                for (index, file) in file_set.iter().enumerate() {
                    if let Some(parent_path) = file.parent() {
                        thread_pool.install(|| {
                            mkdir(parent_path).expect("failed to construct directory tree");
                            assert!(parent_path.is_dir());
                            let fd = open.run(
                                file,
                                nix::fcntl::OFlag::O_CREAT | nix::fcntl::OFlag::O_RDWR,
                                nix::sys::stat::Mode::S_IRWXU,
                            ).expect("failed to create file");

                            let file_size = config.file_size_distribution.ind_sample(&mut rng) as usize;
                            if write_buffer.len() < file_size {
                                write_buffer.resize(file_size, 0);
                            }
                            write
                                .run(fd, &write_buffer[0..file_size])
                                .unwrap_or_else(|err| {
                                    panic!(
                                        "error {}, failed to write {} bytes to {:?}, fd {}",
                                        err, file_size, file, fd
                                    )
                                });

                            if maybe_fsync(index) {
                                fsync.run(fd).expect("failed to fsync file");
                            }
                            close.run(fd).expect("failed to close file");
                        });
                    }
                }
                sync.run();
            })
            .expect("failed to record trace");

        info!("Finished micro-create:");
        let open_stats = open.stats.read().unwrap().deref().clone();
        let close_stats = close.stats.read().unwrap().deref().clone();
        let fsync_stats = fsync.stats.read().unwrap().deref().clone();
        let sync_stats = sync.stats.read().unwrap().deref().clone();
        let write_stats = write.stats.read().unwrap().deref().clone();
        info!(" - Open: {}", open_stats);
        info!(" - Close: {}", close_stats);
        info!(" - Fsync: {}", fsync_stats);
        info!(" - Sync: {}", sync_stats);
        info!(" - Write: {}", write_stats);
        info!(
            " - Total: {}",
            open_stats.clone() + close_stats.clone() + fsync_stats.clone() + sync_stats.clone() + write_stats.clone()
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
            write: write_stats,
            trace: trace,
        }
    }
}

use super::*;
use std::path::{Path, PathBuf};
use rand::distributions::IndependentSample;
use rand::Rng;
use std::cmp::min;
use std::ops::Deref;
use std::marker;

#[allow(dead_code)]
pub struct Varmail {
    create: Stats,
    delete: Stats,
    open: Stats,
    write: Stats,
    read: Stats,
    fsync: Stats,
    trace: Trace,
}

pub struct VarmailConfig<R>
where
    R: IndependentSample<f64>,
{
    pub file_size_distribution: R,
    pub append_distribution: R,
    pub iterations: usize,
}

impl Varmail {
    pub fn run<R: IndependentSample<f64> + marker::Sync, RV: IndependentSample<f64> + marker::Sync>(config: &Configuration<R, RV>) -> Self {
        drop_cache();
        let config_path: &Path = config.filesystem_path.as_ref();
        let base_path = PathBuf::from(config_path.join("varmail"));
        let file_set: Vec<PathBuf> = FileSet::new(config.num_files, &base_path, config.dir_width)
            .into_iter()
            .collect();
        let mut createfile2 = Open::new();
        let mut createfile2_write = Write::new();
        let mut deletefile1 = Unlink::new();
        let mut appendfilerand2 = Write::new();
        let mut fsyncfile2 = Fsync::new();
        let mut closefile2 = Close::new();
        let mut openfile3 = Open::new();
        let mut readfile3 = Read::new();
        let mut appendfilerand3 = Write::new();
        let mut fsyncfile3 = Fsync::new();
        let mut closefile3 = Close::new();
        let mut openfile4 = Open::new();
        let mut readfile4 = Read::new();
        let mut closefile4 = Close::new();

        let zero_buffer = [0; 24000];
        let mut read_buffer = [0; 1_000_000];

        let trace = config
            .blktrace
            .record_with(|| {
                let thread_pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(config.num_threads)
                    .build()
                    .unwrap();
                for _ in 0..config.varmail_config.iterations {
                    let mut file: &PathBuf = rand::thread_rng()
                        .choose(&file_set)
                        .expect("failed to select a file");
                    thread_pool.install(|| {
                        if file.exists() {
                            let _ = deletefile1.run(file); // throw away result
                        }
                        mkdir(file.parent().expect("file path does not have a parent")).expect("failed to create directory tree");
                        let mut fd1 = createfile2
                            .run(
                                file,
                                nix::fcntl::OFlag::O_CREAT | nix::fcntl::OFlag::O_APPEND | nix::fcntl::OFlag::O_WRONLY,
                                nix::sys::stat::Mode::S_IRWXU,
                            )
                            .expect("failed to create file in createfile2");
                        let mut filesize = min(
                            config
                                .varmail_config
                                .file_size_distribution
                                .ind_sample(&mut rand::thread_rng()) as usize,
                            zero_buffer.len(),
                        );
                        createfile2_write
                            .run(fd1, &zero_buffer[..filesize])
                            .expect("failed to append to created file");
                        let appendsize = min(
                            config
                                .varmail_config
                                .append_distribution
                                .ind_sample(&mut rand::thread_rng()) as usize,
                            zero_buffer.len(),
                        );
                        appendfilerand2
                            .run(fd1, &zero_buffer[..appendsize])
                            .expect("failed to write to file");
                        fsyncfile2.run(fd1).expect("failed to fsync file");
                        closefile2.run(fd1).expect("failed to close file");

                        file = rand::thread_rng()
                            .choose(&file_set)
                            .expect("failed to select a file");
                        mkdir(file.parent().expect("file path does not have a parent")).expect("failed to create directory tree");
                        fd1 = openfile3
                            .run(
                                file,
                                nix::fcntl::OFlag::O_CREAT | nix::fcntl::OFlag::O_RDWR,
                                nix::sys::stat::Mode::S_IRWXU,
                            )
                            .expect("failed to create file in createfile3");
                        readfile3
                            .run(fd1, &mut read_buffer)
                            .expect("failed to read file");
                        nix::unistd::lseek(fd1, 0, nix::unistd::Whence::SeekEnd).expect("failed to seek to end of file");
                        filesize = min(
                            config
                                .varmail_config
                                .file_size_distribution
                                .ind_sample(&mut rand::thread_rng()) as usize,
                            zero_buffer.len(),
                        );
                        appendfilerand3
                            .run(fd1, &zero_buffer[..filesize])
                            .expect("failed to write to file");
                        fsyncfile3.run(fd1).expect("failed to fsync file");
                        closefile3.run(fd1).expect("failed to close file");

                        file = rand::thread_rng()
                            .choose(&file_set)
                            .expect("failed to select a file");
                        mkdir(file.parent().expect("file path does not have a parent")).expect("failed to create directory tree");
                        fd1 = openfile4
                            .run(
                                file,
                                nix::fcntl::OFlag::O_CREAT | nix::fcntl::OFlag::O_RDONLY,
                                nix::sys::stat::Mode::S_IRWXU,
                            )
                            .expect("failed to create file in createfile2");
                        readfile4
                            .run(fd1, &mut read_buffer)
                            .expect("failed to read file");
                        closefile4.run(fd1).expect("failed to close file");
                    })
                }
            })
            .expect("failed to record trace");

        let create_stats = createfile2.stats.read().unwrap().deref().clone() + createfile2_write.stats.read().unwrap().deref().clone();
        let delete_stats = deletefile1.stats.read().unwrap().deref().clone();
        let open_stats = openfile3.stats.read().unwrap().deref().clone() + openfile4.stats.read().unwrap().deref().clone();
        let write_stats = appendfilerand2.stats.read().unwrap().deref().clone() + appendfilerand3.stats.read().unwrap().deref().clone();
        let read_stats = readfile3.stats.read().unwrap().deref().clone() + readfile4.stats.read().unwrap().deref().clone();
        let fsync_stats = fsyncfile2.stats.read().unwrap().deref().clone() + fsyncfile3.stats.read().unwrap().deref().clone();
        info!("Completed varmail benchmark");
        info!("Create stats: {}", create_stats);
        info!("Delete stats: {}", delete_stats);
        info!("Open stats: {}", open_stats);
        info!("Write stats: {}", write_stats);
        info!("Read stats: {}", read_stats);
        info!("Fsync stats: {}", fsync_stats);
        info!("Total: {}", create_stats.clone() + delete_stats.clone() + open_stats.clone() + write_stats.clone() + read_stats.clone() + fsync_stats.clone());
        trace.export(&config.output_dir, &"varmail");

        Varmail {
            create: create_stats,
            delete: delete_stats,
            open: open_stats,
            write: write_stats,
            read: read_stats,
            fsync: fsync_stats,
            trace: trace,
        }
    }
}

use super::nix;
use super::rand;
use super::rayon;
use super::fsbench::operation::*;
use super::fsbench::statistics::*;
use super::fsbench::blktrace::*;
use super::fsbench::util::*;
use super::fsbench::fileset::*;
use super::BaseConfiguration;
use super::serde_json;
use std::io;
use std::path::{Path, PathBuf};
use rand::distributions::Gamma;
use rand::Rng;
use std::cmp::min;

pub struct Varmail<'a> {
    create: Stats,
    delete: Stats,
    open: Stats,
    write: Stats,
    read: Stats,
    fsync: Stats,
    trace: Trace,
    base_config: &'a BaseConfiguration<'a>,
    varmail_config: &'a VarmailConfig,
}

#[derive(Serialize, Deserialize)]
pub struct VarmailConfig {
    pub num_files: usize,
    pub dir_width: usize,
    pub file_size_distribution: (f64, f64),
    pub append_distribution: (f64, f64),
    pub iterations: usize,
    pub num_threads: usize,
}

use std::error::Error;

impl VarmailConfig {
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, Box<Error>> {
        use super::serde_json;
        use std::fs::File;
        let file = File::open(path)?;
        let c = serde_json::from_reader(file)?;
        Ok(c)
    }
}

impl Default for VarmailConfig {
    fn default() -> Self {
        Self {
            num_files: super::DEFAULT_NUM_FILES,
            dir_width: super::DEFAULT_DIR_WIDTH,
            file_size_distribution: (10000.0, 3000.0),
            append_distribution: (10000.0, 3000.0),
            iterations: 50000,
            num_threads: 4,
        }
    }
}

impl<'a> Varmail<'a> {
    pub fn run(base_config: &'a BaseConfiguration, varmail_config: &'a VarmailConfig) -> Self {
        use rand::distributions::IndependentSample;

        drop_cache();
        let config_path: &Path = base_config.filesystem_path.as_ref();
        let base_path = PathBuf::from(config_path.join("varmail"));
        let file_set: Vec<PathBuf> = FileSet::new(
            varmail_config.num_files,
            &base_path,
            varmail_config.dir_width,
        ).into_iter()
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

        let file_size_distribution = Gamma::new(
            varmail_config.file_size_distribution.0,
            varmail_config.file_size_distribution.1,
        );
        let append_distribution = Gamma::new(
            varmail_config.append_distribution.0,
            varmail_config.append_distribution.1,
        );

        let trace = base_config
            .blktrace
            .record_with(|| {
                let thread_pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(varmail_config.num_threads)
                    .build()
                    .unwrap();
                for _ in 0..varmail_config.iterations {
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
                            file_size_distribution.ind_sample(&mut rand::thread_rng()) as usize,
                            zero_buffer.len(),
                        );
                        createfile2_write
                            .run(fd1, &zero_buffer[..filesize])
                            .expect("failed to append to created file");
                        let appendsize = min(
                            append_distribution.ind_sample(&mut rand::thread_rng()) as usize,
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
                            file_size_distribution.ind_sample(&mut rand::thread_rng()) as usize,
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

        let create_stats = createfile2.get_stats() + createfile2_write.get_stats();
        let delete_stats = deletefile1.get_stats();
        let open_stats = openfile3.get_stats() + openfile4.get_stats();
        let write_stats = appendfilerand2.get_stats() + appendfilerand3.get_stats();
        let read_stats = readfile3.get_stats() + readfile4.get_stats();
        let fsync_stats = fsyncfile2.get_stats() + fsyncfile3.get_stats();
        info!("Completed varmail benchmark");
        info!("Create stats: {}", create_stats);
        info!("Delete stats: {}", delete_stats);
        info!("Open stats: {}", open_stats);
        info!("Write stats: {}", write_stats);
        info!("Read stats: {}", read_stats);
        info!("Fsync stats: {}", fsync_stats);
        info!(
            "Total: {}",
            create_stats.clone() + delete_stats.clone() + open_stats.clone() + write_stats.clone() + read_stats.clone()
                + fsync_stats.clone()
        );

        Varmail {
            create: create_stats,
            delete: delete_stats,
            open: open_stats,
            write: write_stats,
            read: read_stats,
            fsync: fsync_stats,
            trace: trace,
            base_config: base_config,
            varmail_config: varmail_config,
        }
    }

    pub fn export(&self) -> io::Result<()> {
        let path = self.base_config.output_dir.join("varmail");
        use std::fs::File;
        mkdir(&path)?;
        serde_json::to_writer(File::create(path.join("create.json"))?, &self.create)?;
        serde_json::to_writer(File::create(path.join("delete.json"))?, &self.delete)?;
        serde_json::to_writer(File::create(path.join("open.json"))?, &self.open)?;
        serde_json::to_writer(File::create(path.join("write.json"))?, &self.write)?;
        serde_json::to_writer(File::create(path.join("read.json"))?, &self.read)?;
        serde_json::to_writer(File::create(path.join("fsync.json"))?, &self.fsync)?;
        serde_json::to_writer(
            File::create(path.join("config.json"))?,
            &self.varmail_config,
        )?;
        self.trace.export(&path, &"blktrace")
    }

    pub fn total(&self) -> Stats {
        self.create.clone() + self.delete.clone() + self.open.clone() + self.write.clone() + self.read.clone() + self.fsync.clone()
    }
}

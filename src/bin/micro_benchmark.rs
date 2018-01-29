extern crate chrono;
extern crate clap;
extern crate fern;
extern crate fsbench;
#[macro_use]
extern crate log;
extern crate rand;
use fsbench::*;
use rand::Rng;
use std::path::{Path, PathBuf};

/*
Notes:
How large should the created files be?
*/

fn main() {
    setup_logger().expect("failed to setup logger");
    let matches = clap::App::new("Micro Benchmark")
        .version("0.1")
        .about("Runs micro benchmarks on a filesystem")
        .arg(
            clap::Arg::with_name("PATH")
                .short("p")
                .long("path")
                .help("Path to filesystem mount point (default = /mnt)")
                .takes_value(true),
        )
        .arg(
            clap::Arg::with_name("NUM_FILES")
                .short("n")
                .long("num-files")
                .help("Number of files to generate (default = 1000000)")
                .takes_value(true),
        )
        .arg(
            clap::Arg::with_name("DIR_WIDTH")
                .short("w")
                .long("dir-width")
                .help("Directory width (default = 7)")
                .takes_value(true),
        )
        .get_matches();

    sync_drop_cache();
    let filesystem_path = matches.value_of("PATH").unwrap_or("/mnt");
    let num_files = matches.value_of("NUM_FILES").map_or(100000, |s| {
        s.parse().expect("Failed to parse number of files")
    });
    let dir_width = matches
        .value_of("DIR_WIDTH")
        .map_or(7, |s| s.parse().expect("Failed to parse directory width"));
    info!(
        "Running benchmark on {:?} with {} files",
        filesystem_path, num_files
    );
    info!("Running create test (no fsync)..");
    create_test(
        &filesystem_path,
        num_files,
        dir_width,
        &"create_no_fsync",
        |_| false,
        false,
    );
    info!("Running create test (end sync)..");
    create_test(
        &filesystem_path,
        num_files,
        dir_width,
        &"create_end_sync",
        |_| false,
        true,
    );
    info!("Running create test (intermittent fsync)..");
    create_test(
        &filesystem_path,
        num_files,
        dir_width,
        &"create_intermittent_fsync",
        |index| index % 10 == 0,
        false,
    );
    info!("Running create test (intermittent fsync with end sync)..");
    create_test(
        &filesystem_path,
        num_files,
        dir_width,
        &"create_intermittent_fsync_with_end_sync",
        |index| index % 10 == 0,
        true,
    );
    info!("Running create test (frequent fsync)..");
    create_test(
        &filesystem_path,
        num_files,
        dir_width,
        &"create_frequent_fsync",
        |_| true,
        false,
    );
    info!("Running create test (frequent fsync with end sync)..");
    create_test(
        &filesystem_path,
        num_files,
        dir_width,
        &"create_frequent_fsync_with_end_sync",
        |_| true,
        true,
    );
    info!("Running rename test..");
    rename_test(&filesystem_path, num_files, dir_width);
    info!("Running delete test..");
    delete_test(&filesystem_path, num_files, dir_width);
}

fn create_test<P: AsRef<Path>, N: AsRef<Path>, F: Fn(usize) -> bool>(
    path: &P,
    num_files: usize,
    dir_width: usize,
    name: &N,
    maybe_fsync: F,
    should_sync: bool,
) {
    sync_drop_cache();
    let base_path = PathBuf::from(path.as_ref().join(name));
    let file_set: Vec<PathBuf> = FileSet::new(num_files, &base_path, dir_width)
        .into_iter()
        .collect();
    let mut open = Open::new();
    let mut close = Close::new();
    let mut fsync = Fsync::new();
    let mut sync = Sync::new();
    // Create directory structure and files
    for (index, file) in file_set.into_iter().enumerate() {
        if let Some(parent_path) = file.parent() {
            mkdir(parent_path).expect("failed to construct directory tree");
            assert!(parent_path.is_dir());
            let fd = open.run(
                &file,
                nix::fcntl::OFlag::O_CREAT,
                nix::sys::stat::Mode::S_IRWXU,
            ).expect("failed to create file");
            if maybe_fsync(index) {
                fsync.run(fd).expect("failed to fsync file");
            }
            close.run(fd).expect("failed to close file");
        }
    }

    if should_sync {
        sync.run();
    }

    info!("Finished micro-create:");
    info!(" - Open: {}", open.stats);
    info!(" - Close: {}", close.stats);
    info!(" - Fsync: {}", fsync.stats);
    info!(" - Sync: {}", sync.stats);
    info!(
        " - Total: {}",
        open.stats + close.stats + fsync.stats + sync.stats
    );
    sync_drop_cache();
}

fn delete_test<P: AsRef<Path>>(path: &P, num_files: usize, dir_width: usize) {
    sync_drop_cache();
    let base_path = PathBuf::from(path.as_ref().join("delete"));
    let file_set: Vec<PathBuf> = FileSet::new(num_files, &base_path, dir_width)
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

    sync_drop_cache();

    for file in file_set_shuffled {
        unlink.run(&file).expect("failed to unlink file");
    }
    info!("Finished micro-delete:");
    info!(" - Open: {}", open.stats);
    info!(" - Close: {}", close.stats);
    info!(" - Unlink: {}", unlink.stats);
    info!(" - Total: {}", open.stats + close.stats + unlink.stats);
    sync_drop_cache();
}

fn rename_test<P: AsRef<Path>>(path: &P, num_files: usize, dir_width: usize) {
    sync_drop_cache();
    let base_path = PathBuf::from(path.as_ref().join("rename"));
    let file_set: Vec<PathBuf> = FileSet::new(num_files, &base_path, dir_width)
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

    sync_drop_cache();

    for file in file_set_shuffled {
        // Rename /path/to/file to /path/to/file.data
        let new_path = file.with_extension("data");
        rename.run(&file, &new_path).expect("failed to rename file");
    }

    info!("Finished micro-delete:");
    info!(" - Open: {}", open.stats);
    info!(" - Close: {}", close.stats);
    info!(" - Rename: {}", rename.stats);
    info!(" - Total: {}", open.stats + close.stats + rename.stats);
    sync_drop_cache();
    /*
    Notes:
    Did not specify how to rename
    */
}

fn sync_drop_cache() {
    sync_all();
    if let Err(e) = drop_cache() {
        warn!("failed to drop cache (maybe run with sudo?): {:?}", e);
    }
}

fn setup_logger() -> Result<(), fern::InitError> {
    use fern::colors::{Color, ColoredLevelConfig};
    fern::Dispatch::new()
        .format(|out, message, record| {
            let colors = ColoredLevelConfig::new()
                .info(Color::Green)
                .warn(Color::Yellow);
            out.finish(format_args!(
                "{}[{}][{}] {}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.target(),
                colors.color(record.level()),
                message
            ))
        })
        .level(log::LevelFilter::Debug)
        .chain(std::io::stdout())
        .apply()?;
    Ok(())
}

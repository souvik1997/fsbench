extern crate chrono;
extern crate clap;
extern crate fern;
extern crate fsbench;
extern crate nix;
extern crate tempdir;
#[macro_use]
extern crate log;
extern crate rand;
use fsbench::*;
use rand::distributions::IndependentSample;
use rand::distributions::normal::Normal;
use std::path::{Path, PathBuf};
use std::process::Command;


mod benchmarks;

/*
Notes:
How large should the created files be?
*/


pub struct Configuration<'a, R> where R: IndependentSample<f64> {
    filesystem_path: &'a Path,
    num_files: usize,
    dir_width: usize,
    file_size_distribution: R,
    num_threads: usize,
    blktrace: Blktrace,
    output_dir: PathBuf,
}

fn main() {
    setup_logger().expect("failed to setup logger");
    let matches = clap::App::new("Filesystem Benchmark")
        .version("0.1")
        .about("Runs benchmarks on a filesystem")
        .arg(
            clap::Arg::with_name("DEVICE")
                .short("d")
                .long("device")
                .help("Block device to run tests on (note: must be unmounted)")
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
        .arg(
            clap::Arg::with_name("OUTPUT")
                .short("o")
                .long("output-directory")
                .help("Output directory (default = './output)")
                .takes_value(true),
        )
        .arg(
            clap::Arg::with_name("DEBUGFS")
                .short("k")
                .long("debugfs-path")
                .help("debugfs path (default = '/sys/kernel/debug')")
                .takes_value(true),
        )
        .get_matches();

    let uid = nix::unistd::geteuid();
    if !uid.is_root() {
        error!("Need to be root");
        return;
    }

    let device = matches.value_of("DEVICE").expect("No device specified");
    let tempdir = tempdir::TempDir::new("benchmarks").expect("failed to create temporary directory");
    let filesystem_path = tempdir.path();
    let output_dir = PathBuf::from(matches.value_of("OUTPUT").unwrap_or("./output"));
    let debugfs_path = matches.value_of("DEBUGFS").unwrap_or("/sys/kernel/debug");
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
    let blktrace = Blktrace::new(PathBuf::from(device), BlktraceConfig::default(), debugfs_path).expect("failed to setup blktrace");

    let config = Configuration {
        filesystem_path: &filesystem_path,
        num_files: num_files,
        dir_width: dir_width,
        file_size_distribution: Normal::new(100 as f64, 10 as f64),
        num_threads: 8,
        blktrace: blktrace,
        output_dir: output_dir,
    };

    if !Command::new("mount").args(&[device, filesystem_path.to_str().expect("failed to convert path to string")]).status().expect("failed to run `mount`").success() {
        error!("failed to mount {} on {:?}", device, filesystem_path);
        return;
    }
    info!("Mounted {} at {:?}", device, filesystem_path);
    drop_cache();

    info!("Running create test (end sync)..");
    let create_end_sync = benchmarks::CreateFiles::run(
        &config,
        &"create_end_sync",
        |_| false,
    );
    info!("Running create test (intermittent fsync with end sync)..");
    let create_intermittent_sync = benchmarks::CreateFiles::run(
        &config,
        &"create_intermittent_fsync_with_end_sync",
        |index| index % 10 == 0,
    );
    info!("Running create test (frequent fsync with end sync)..");
    let create_freq_sync = benchmarks::CreateFiles::run(
        &config,
        &"create_frequent_fsync_with_end_sync",
        |_| true,
    );
    info!("Running rename test..");
    let rename_files = benchmarks::RenameFiles::run(&config);
    info!("Running delete test..");
    let delete_files = benchmarks::DeleteFiles::run(&config);
    info!("Running listdir test..");
    let listdir = benchmarks::ListDir::run(&config);
    if !Command::new("umount").args(&[filesystem_path.to_str().expect("failed to convert path to string")]).status().expect("failed to run `mount`").success() {
        error!("failed to unmount {:?}", filesystem_path);
        return;
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

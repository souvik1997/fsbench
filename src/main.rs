extern crate chrono;
extern crate clap;
extern crate fern;
#[macro_use]
extern crate log;
extern crate nix;
extern crate rand;
extern crate rayon;
extern crate tempdir;

mod benchmarks;
mod fsbench;

fn main() {
    use fsbench::blktrace::*;
    use fsbench::util::drop_cache;
    use rand::distributions::normal::Normal;
    use rand::distributions::Gamma;
    use std::path::PathBuf;
    use std::process::Command;
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

    // we need to be root to use blktrace and mount filesystems
    let uid = nix::unistd::geteuid();
    if !uid.is_root() {
        error!("Need to be root");
        return;
    }

    // Get the command line arguments
    // device = the block device to test (e.g. /dev/sda1, /dev/nvme0n1)
    // We expect the device to _not_ be mounted
    let device = matches.value_of("DEVICE").expect("No device specified");

    // Create a temporary directory. The device will be mounted here
    let tempdir = tempdir::TempDir::new("benchmarks").expect("failed to create temporary directory");
    // Get the path of the temporary directory
    let filesystem_path = tempdir.path();

    // All results will be written to the output directory
    let output_dir = PathBuf::from(matches.value_of("OUTPUT").unwrap_or("./output"));

    // The path where debugfs is mounted. This is used for blktrace
    let debugfs_path = matches.value_of("DEBUGFS").unwrap_or("/sys/kernel/debug");

    // Get the number of files to create and directory width
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

    // Start blktrace. This will call BLKTRACESETUP and BLKTRACESTART so IO events
    // will start showing up. However we will only consider events that occur during the benchmarks
    let blktrace = Blktrace::new(
        PathBuf::from(device),
        BlktraceConfig::default(),
        debugfs_path,
    ).expect("failed to setup blktrace");

    let config = benchmarks::Configuration {
        filesystem_path: &filesystem_path,
        num_files: num_files,
        dir_width: dir_width,
        file_size_distribution: Normal::new(100 as f64, 10 as f64),
        num_threads: 8,
        blktrace: blktrace,
        output_dir: output_dir,
        varmail_config: benchmarks::VarmailConfig {
            file_size_distribution: Gamma::new(16384 as f64, 1.5 as f64),
            append_distribution: Gamma::new(16384 as f64, 1.5 as f64),
            iterations: 10000,
        },
    };

    // Mount the device at the mountpoint using the `mount` command
    // NOTE: we could use mount(2), but that doesn't auto-detect the filesystem
    // which means we would have to try each filesystem that the kernel supports.
    // mount returns with exit code 0 if it succeeds.
    if !Command::new("mount")
        .args(&[
            device,
            filesystem_path
                .to_str()
                .expect("failed to convert path to string"),
        ])
        .status()
        .expect("failed to run `mount`")
        .success()
    {
        error!("failed to mount {} on {:?}", device, filesystem_path);
        return;
    }
    info!("Mounted {} at {:?}", device, filesystem_path);

    drop_cache();

    // Standard createfiles test with no fsync
    info!("Running create test (end sync)..");
    let _createfiles = benchmarks::CreateFiles::run(&config, &"createfiles", None);

    // Create files, but fsync after every 10 files
    info!("Running create test (intermittent fsync)..");
    let _createfiles_sync = benchmarks::CreateFiles::run(&config, &"createfiles_sync", Some(10));

    // Create files, but fsync after every file
    info!("Running create test (frequent fsync)..");
    let _create_freq_sync = benchmarks::CreateFiles::run(&config, &"createfiles_eachsync", Some(1));

    // Rename files test
    info!("Running rename test..");
    let _rename_files = benchmarks::RenameFiles::run(&config);

    // Delete files test
    // NOTE: filebench has a removedirs.f workload, but this actually only calls rmdir() and _does not_
    // recursively delete files
    info!("Running delete test..");
    let _delete_files = benchmarks::DeleteFiles::run(&config);

    // Listdir test
    info!("Running listdir test..");
    let _listdir = benchmarks::ListDir::run(&config);

    // Varmail test, based off varmail.f from filebench
    info!("Running varmail test..");
    let _varmail = benchmarks::Varmail::run(&config);

    // Unmount the device
    drop_cache();
    if !Command::new("umount")
        .args(&[
            filesystem_path
                .to_str()
                .expect("failed to convert path to string"),
        ])
        .status()
        .expect("failed to run `mount`")
        .success()
    {
        error!("failed to unmount {:?}", filesystem_path);
        return;
    }

    // Blktrace will be stopped by its destructor
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

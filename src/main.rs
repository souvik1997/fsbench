extern crate chrono;
extern crate clap;
extern crate fern;
#[macro_use]
extern crate log;
extern crate nix;
extern crate rand;
extern crate rayon;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate tempdir;

mod benchmarks;
mod fsbench;

fn main() {
    // Enable backtraces
    ::std::env::set_var("RUST_BACKTRACE", "1");

    use fsbench::blktrace::*;
    use fsbench::util::drop_cache;
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
        .arg(
            clap::Arg::with_name("MOUNT_PATH")
                .short("m")
                .long("mount-path")
                .help("where to mount the block device")
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
    let filesystem_path = matches
        .value_of("MOUNT_PATH")
        .map_or(tempdir.path().to_owned(), |s| PathBuf::from(s));

    // All results will be written to the output directory
    let output_dir = PathBuf::from(matches.value_of("OUTPUT").unwrap_or("./output"));

    // The path where debugfs is mounted. This is used for blktrace
    let debugfs_path = matches.value_of("DEBUGFS").unwrap_or("/sys/kernel/debug");

    // Start blktrace. This will call BLKTRACESETUP and BLKTRACESTART so IO events
    // will start showing up. However we will only consider events that occur during the benchmarks
    let blktrace = Blktrace::new(
        PathBuf::from(device),
        BlktraceConfig::default(),
        debugfs_path,
    ).expect("failed to setup blktrace");

    let base_config = benchmarks::BaseConfiguration {
        filesystem_path: &filesystem_path,
        blktrace: blktrace,
        output_dir: output_dir,
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
    let createfiles_config =
        benchmarks::CreateFilesConfig::load("createfiles_config.json").unwrap_or(benchmarks::CreateFilesConfig::default());
    let createfiles = benchmarks::CreateFiles::run(&base_config, &createfiles_config);
    createfiles
        .export()
        .expect("failed to export benchmark data");

    // Create files, but fsync after every 10 files
    info!("Running create test (intermittent fsync)..");
    let createfiles_sync_config = benchmarks::CreateFilesBatchSyncConfig::load("createfiles_batchsync.json")
        .unwrap_or(benchmarks::CreateFilesBatchSyncConfig::default());
    let createfiles_sync = benchmarks::CreateFilesBatchSync::run(&base_config, &createfiles_sync_config);
    createfiles_sync
        .export()
        .expect("failed to export benchmark data");

    // Create files, but fsync after every file
    info!("Running create test (frequent fsync)..");
    let createfiles_eachsync_config = benchmarks::CreateFilesEachSyncConfig::load("createfiles_eachsync.json")
        .unwrap_or(benchmarks::CreateFilesEachSyncConfig::default());
    let createfiles_eachsync = benchmarks::CreateFilesEachSync::run(&base_config, &createfiles_eachsync_config);
    createfiles_eachsync
        .export()
        .expect("failed to export benchmark data");

    // Rename files test
    info!("Running rename test..");
    let renamefiles_config =
        benchmarks::RenameFilesConfig::load("renamefiles_config.json").unwrap_or(benchmarks::RenameFilesConfig::default());
    let renamefiles = benchmarks::RenameFiles::run(&base_config, &renamefiles_config);
    renamefiles
        .export()
        .expect("failed to export benchmark data");

    // Delete files test
    // NOTE: filebench has a removedirs.f workload, but this actually only calls rmdir() and _does not_
    // recursively delete files
    info!("Running delete test..");
    let deletefiles_config =
        benchmarks::DeleteFilesConfig::load("deletefiles_config.json").unwrap_or(benchmarks::DeleteFilesConfig::default());
    let deletefiles = benchmarks::DeleteFiles::run(&base_config, &deletefiles_config);
    deletefiles
        .export()
        .expect("failed to export benchmark data");

    // Listdir test
    info!("Running listdir test..");
    let listdir_config = benchmarks::ListDirConfig::load("listdir_config.json").unwrap_or(benchmarks::ListDirConfig::default());
    let listdir = benchmarks::ListDir::run(&base_config, &listdir_config);
    listdir.export().expect("failed to export benchmark data");

    // Varmail test, based off varmail.f from filebench
    info!("Running varmail test..");
    let varmail_config = benchmarks::VarmailConfig::load("varmail_config.json").unwrap_or(benchmarks::VarmailConfig::default());
    let varmail = benchmarks::Varmail::run(&base_config, &varmail_config);
    varmail.export().expect("failed to export benchmark data");

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

    use std::fs::File;
    let mut summary = File::create(base_config.output_dir.join("summary.txt")).expect("failed to create summary file");
    print_summary(&mut summary, "Createfiles", &createfiles).expect("failed to write to summary");
    print_summary(&mut summary, "Createfiles Batch Sync", &createfiles_sync).expect("failed to write to summary");
    print_summary(&mut summary, "Createfiles Each Sync", &createfiles_eachsync).expect("failed to write to summary");
    print_summary(&mut summary, "Renamefiles", &renamefiles).expect("failed to write to summary");
    print_summary(&mut summary, "Deletefiles", &deletefiles).expect("failed to write to summary");
    print_summary(&mut summary, "Listdir", &listdir).expect("failed to write to summary");
    print_summary(&mut summary, "Varmail", &varmail).expect("failed to write to summary");

    // Blktrace will be stopped by its destructor
}

fn print_summary<S: ::std::fmt::Display, B: benchmarks::Benchmark, W: ::std::io::Write>(
    writer: &mut W,
    name: S,
    benchmark: &B,
) -> ::std::io::Result<()> {
    let total = benchmark.total();
    let reads = benchmark.get_trace().completed_reads;
    let writes = benchmark.get_trace().completed_writes;
    writeln!(
        writer,
        "{}: Operations: {}, Operations/Second: {}, Reads: {}, Writes: {}, Reads/Operation: {}, Writes/Operation: {}",
        name,
        total.num_ops(),
        total.ops_per_second(),
        reads,
        writes,
        (reads as f64) / (total.num_ops() as f64),
        (writes as f64) / (total.num_ops() as f64)
    )
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

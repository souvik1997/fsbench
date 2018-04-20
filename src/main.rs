#![feature(duration_extras)]

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
extern crate num;
extern crate serde_json;
extern crate tempdir;
#[macro_use]
extern crate num_derive;
extern crate num_traits;
#[macro_use]
extern crate bitflags;

mod benchmarks;
mod fsbench;

use std::time::Duration;

fn main() {
    // Enable backtraces
    ::std::env::set_var("RUST_BACKTRACE", "1");

    use fsbench::blktrace::*;
    use fsbench::mount::Mount;
    use fsbench::util::{drop_cache, mkfs, Filesystem};
    use std::path::PathBuf;
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
    let filesystem_path_str = filesystem_path.to_str().expect("failed to convert path to str");

    // All results will be written to the output directory
    let output_dir = PathBuf::from(matches.value_of("OUTPUT").unwrap_or("./output"));

    // The path where debugfs is mounted. This is used for blktrace
    let debugfs_path = matches.value_of("DEBUGFS").unwrap_or("/sys/kernel/debug");

    // Start blktrace. This will call BLKTRACESETUP and BLKTRACESTART so IO events
    // will start showing up. However we will only consider events that occur during the benchmarks
    let blktrace = Blktrace::new(PathBuf::from(device), BlktraceConfig::default(), debugfs_path).expect("failed to setup blktrace");





    let filesystems = [Filesystem::Ext2, Filesystem::Ext4, Filesystem::Ext4NoJournal, Filesystem::Xfs, Filesystem::Btrfs, Filesystem::F2fs];
    for fstype in filesystems.into_iter() {
        let base_config = benchmarks::BaseConfiguration {
            filesystem_path: &filesystem_path,
            blktrace: &blktrace,
            output_dir: output_dir.join(fstype.to_string()),
        };

        drop_cache();

        // Standard createfiles test with no fsync
        let createfiles_config =
            benchmarks::CreateFilesConfig::load("createfiles_config.json").unwrap_or(benchmarks::CreateFilesConfig::default());

        let createfiles = {
            mkfs(device, fstype);
            let m = Mount::new(device, filesystem_path_str);
            info!("Running create test (end sync)..");
            let createfiles = benchmarks::CreateFiles::run(&base_config, &createfiles_config);
            createfiles.export().expect("failed to export benchmark data");
            createfiles
        };


        let createfiles_sync_config = benchmarks::CreateFilesBatchSyncConfig::load("createfiles_batchsync.json")
            .unwrap_or(benchmarks::CreateFilesBatchSyncConfig::default());
        let createfiles_sync = {
            mkfs(device, fstype);
            let m = Mount::new(device, filesystem_path_str);
            // Create files, but fsync after every 10 files
            info!("Running create test (intermittent fsync)..");
            let createfiles_sync = benchmarks::CreateFilesBatchSync::run(&base_config, &createfiles_sync_config);
            createfiles_sync.export().expect("failed to export benchmark data");
            createfiles_sync
        };

        let createfiles_eachsync_config = benchmarks::CreateFilesEachSyncConfig::load("createfiles_eachsync.json")
            .unwrap_or(benchmarks::CreateFilesEachSyncConfig::default());
        let createfiles_eachsync = {
            mkfs(device, fstype);
            let m = Mount::new(device, filesystem_path_str);
            // Create files, but fsync after every file
            info!("Running create test (frequent fsync)..");
            let createfiles_eachsync = benchmarks::CreateFilesEachSync::run(&base_config, &createfiles_eachsync_config);
            createfiles_eachsync.export().expect("failed to export benchmark data");
            createfiles_eachsync
        };

        let renamefiles_config =
            benchmarks::RenameFilesConfig::load("renamefiles_config.json").unwrap_or(benchmarks::RenameFilesConfig::default());
        let renamefiles = {
            mkfs(device, fstype);
            let m = Mount::new(device, filesystem_path_str);
            // Rename files test
            info!("Running rename test..");
            let renamefiles = benchmarks::RenameFiles::run(&base_config, &renamefiles_config);
            renamefiles.export().expect("failed to export benchmark data");
            renamefiles
        };

        let deletefiles_config =
            benchmarks::DeleteFilesConfig::load("deletefiles_config.json").unwrap_or(benchmarks::DeleteFilesConfig::default());
        let deletefiles =  {
            mkfs(device, fstype);
            let m = Mount::new(device, filesystem_path_str);
            // Delete files test
            // NOTE: filebench has a removedirs.f workload, but this actually only calls rmdir() and _does not_
            // recursively delete files
            info!("Running delete test..");
            let deletefiles = benchmarks::DeleteFiles::run(&base_config, &deletefiles_config);
            deletefiles.export().expect("failed to export benchmark data");
            deletefiles
        };

        let listdir_config = benchmarks::ListDirConfig::load("listdir_config.json").unwrap_or(benchmarks::ListDirConfig::default());
        let listdir = {
            mkfs(device, fstype);
            let m = Mount::new(device, filesystem_path_str);
            // Listdir test
            info!("Running listdir test..");
            let listdir = benchmarks::ListDir::run(&base_config, &listdir_config);
            listdir.export().expect("failed to export benchmark data");
            listdir
        };

        /*
        // Varmail test, based off varmail.f from filebench
        info!("Running varmail test..");
        let varmail_config = benchmarks::VarmailConfig::load("varmail_config.json").unwrap_or(benchmarks::VarmailConfig::default());
        let varmail = benchmarks::Varmail::run(&base_config, &varmail_config);
        varmail.export().expect("failed to export benchmark data");
         */

        use std::fs::File;
        let info = vec![
            get_summary("createfiles", &createfiles),
            get_summary("createfiles_batchsync", &createfiles_sync),
            get_summary("createfiles_eachsync", &createfiles_eachsync),
            get_summary("renamefiles", &renamefiles),
            get_summary("deletefiles", &deletefiles),
            get_summary("listdir", &listdir),
        ];
        serde_json::to_writer(
            File::create(base_config.output_dir.join("summary.json")).expect("failed to create file"),
            &info,
        ).expect("failed to write to summary json");
    }


    // Blktrace will be stopped by its destructor
}

#[derive(Serialize)]
struct Summary {
    name: String,
    duration: Duration,
    io_duration: Duration,
    io_requests: usize,
    operations: usize,
    reads: usize,
    writes: usize,
}

fn get_summary<B: benchmarks::Benchmark>(name: &str, benchmark: &B) -> Summary {
    let total = benchmark.total();
    let reads = benchmark.get_trace().completed_reads();
    let writes = benchmark.get_trace().completed_writes();
    Summary {
        name: name.to_owned(),
        duration: total.total_latency(),
        io_duration: benchmark.get_trace().io_duration(),
        io_requests: benchmark.get_trace().num_requests(),
        operations: total.num_ops(),
        reads: reads,
        writes: writes,
    }
}

fn setup_logger() -> Result<(), fern::InitError> {
    use fern::colors::{Color, ColoredLevelConfig};
    fern::Dispatch::new()
        .format(|out, message, record| {
            let colors = ColoredLevelConfig::new().info(Color::Green).warn(Color::Yellow);
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

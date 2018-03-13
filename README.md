# `fsbench`: a filesystem benchmarking tool

`fsbench` runs a series of benchmarks and runs `blktrace` in the background to collect IO operation information.

## Usage:

```
Filesystem Benchmark 0.1
Runs benchmarks on a filesystem

USAGE:
    fsbench [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -k, --debugfs-path <DEBUGFS>       debugfs path (default = '/sys/kernel/debug')
    -d, --device <DEVICE>              Block device to run tests on (note: must be unmounted)
    -m, --mount-path <MOUNT_PATH>      where to mount the block device
    -o, --output-directory <OUTPUT>    Output directory (default = './output)
    
```

## Building

1. Install stable Rust from https://rustup.rs
2. Run `cargo build --release`
3. Run `target/release/fsbench -d $DEVICE`
4. View output in `./output`

## External Dependencies
`fsbench` needs access to the `mount` and `umount` binaries and needs Linux kernel version > 2.6.31 with `CONFIG_BLK_DEV_IO_TRACE` enabled.

`blkparse` is an optional dependency: `fsbench` will use it if it is available.

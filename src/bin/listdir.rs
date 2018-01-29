extern crate fsbench;
extern crate rand;
use fsbench::*;
use rand::Rng;
use std::path::PathBuf;

fn main() {
    // TODO: not complete
    sync_all();
    if let Err(e) = drop_cache() {
        println!("failed to drop cache (maybe run with sudo?): {:?}", e);
    }
    let base_path = PathBuf::from("/mnt/test");
    let file_set: Vec<PathBuf> = FileSet::new(100000, &base_path, 5).into_iter().collect();
    let mut directories = Vec::<PathBuf>::new();
    let mut readdir = ReadDir::new();
    let mut open = Open::new();
    let mut close = Close::new();
    // Create directory structure and files
    for file in file_set {
        if let Some(parent_path) = file.parent() {
            let mut p = parent_path.to_owned();
            while p != base_path {
                directories.push(p.clone());
                p.pop();
            }
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

    sync_all();
    if let Err(e) = drop_cache() {
        println!("failed to drop cache (maybe run with sudo?): {:?}", e);
    }

    let iterations = 1000000;
    for _ in 0..iterations {
        let directory = rand::thread_rng()
            .choose(&directories)
            .expect("failed to randomly select directory");
        readdir.run(directory).expect("failed to read directory");
    }
    println!("Finished listdir:");
    println!(" - Open: {}", open.stats);
    println!(" - Close: {}", close.stats);
    println!(" - ReadDir: {}", readdir.stats);
    /*
    How many directories should be read? If cache is not dropped between reading directories, then numbers could be skewed
    */
}

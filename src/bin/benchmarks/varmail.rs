extern crate fsbench;
extern crate rand;
extern crate nix;
use fsbench::*;
use rand::Rng;
use rand::distributions::Sample;
use std::path::PathBuf;
use std::cmp::min;
use std::ops::Deref;

fn main() {
    // TODO: clean up code
    // Sync and drop caches
    sync_all();
    if let Err(e) = drop_cache() {
        println!("failed to drop cache (maybe run with sudo?): {:?}", e);
    }
    let base_path = PathBuf::from("/mnt/test");
    let file_set: Vec<PathBuf> = FileSet::new(100000, &base_path, 5).into_iter().collect();
    let mut filesize_distribution = rand::distributions::Gamma::new(16384 as f64, 1.5 as f64);
    let mut append_distribution = rand::distributions::Gamma::new(16384 as f64, 1.5 as f64);
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
    // Create directory structure and files
    let zero_buffer = [0; 24000];
    let mut read_buffer = [0; 1_000_000];
    let iterations = 1000;
    for _ in 0..iterations {
        let mut file: &PathBuf = rand::thread_rng()
            .choose(&file_set)
            .expect("failed to select a file");
        if file.exists() {
            let _ = deletefile1.run(file); // throw away result
        }
        mkdir(file.parent().expect("file path does not have a parent"))
            .expect("failed to create directory tree");
        let mut fd1 = createfile2
            .run(
                file,
                nix::fcntl::OFlag::O_CREAT | nix::fcntl::OFlag::O_APPEND
                    | nix::fcntl::OFlag::O_WRONLY,
                nix::sys::stat::Mode::S_IRWXU,
            )
            .expect("failed to create file in createfile2");
        let mut filesize = min(
            filesize_distribution.sample(&mut rand::thread_rng()) as usize,
            zero_buffer.len(),
        );
        createfile2_write
            .run(fd1, &zero_buffer[..filesize])
            .expect("failed to append to created file");
        let mut appendsize = min(
            append_distribution.sample(&mut rand::thread_rng()) as usize,
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
        mkdir(file.parent().expect("file path does not have a parent"))
            .expect("failed to create directory tree");
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
        nix::unistd::lseek(fd1, 0, nix::unistd::Whence::SeekEnd)
            .expect("failed to seek to end of file");
        filesize = min(
            filesize_distribution.sample(&mut rand::thread_rng()) as usize,
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
        mkdir(file.parent().expect("file path does not have a parent"))
            .expect("failed to create directory tree");
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
    }

    let create_stats = createfile2.stats.read().unwrap().deref().clone() + createfile2_write.stats.read().unwrap().deref().clone();
    let delete_stats = deletefile1.stats.read().unwrap().deref().clone();
    let open_stats = openfile3.stats.read().unwrap().deref().clone() + openfile4.stats.read().unwrap().deref().clone();
    let write_stats = appendfilerand2.stats.read().unwrap().deref().clone() + appendfilerand3.stats.read().unwrap().deref().clone();
    let read_stats = readfile3.stats.read().unwrap().deref().clone() + readfile4.stats.read().unwrap().deref().clone();
    let fsync_stats = fsyncfile2.stats.read().unwrap().deref().clone() + fsyncfile3.stats.read().unwrap().deref().clone();
    println!("Completed varmail benchmark");
    println!("Create stats: {}", create_stats);
    println!("Delete stats: {}", delete_stats);
    println!("Open stats: {}", open_stats);
    println!("Write stats: {}", write_stats);
    println!("Read stats: {}", read_stats);
    println!("Fsync stats: {}", fsync_stats);
    // Does append count as one operation or two?
}

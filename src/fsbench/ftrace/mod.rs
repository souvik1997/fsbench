use super::nix;
use super::super::tempdir::TempDir;
use std::process::Command;
use std::io;
use std::path::{Path, PathBuf};
use std::fs;

/*
 * Run trace-cmd and output to a temporary file, then copy when dumping data
 */


pub struct FtraceConfig {
    pub module: String
}
pub struct Ftracer {
    config: FtraceConfig
}

pub struct Ftrace {
    tmp_dir: TempDir
}

impl Ftracer {
    pub fn new(config: FtraceConfig) -> Self {
        Self {
            config: config,
        }
    }

    pub fn record_with<F: FnMut() -> ()>(&self, mut task: F) -> io::Result<Ftrace> {
        let tmp_dir = TempDir::new(&self.config.module).expect("failed to create temp ftrace directory");
        // let mut child = Command::new("trace-cmd").arg("record").arg("-e").arg(&self.config.module).arg("-o").arg(tmp_dir.path().join("trace.dat")).spawn()?;
        task();
        // nix::sys::signal::kill(nix::unistd::Pid::from_raw(child.id() as i32), nix::sys::signal::SIGTERM).expect("failed to kill process");
        // child.wait()?;
        Ok(Ftrace {
            tmp_dir: tmp_dir
        })
    }
}

impl Ftrace {
    pub fn export<P: AsRef<Path>, Q: AsRef<Path>>(&self, path: &P, filename: &Q) -> io::Result<()> {
        /*
        let mut full_filename = PathBuf::new();
        full_filename.push(path);
        full_filename.push(filename);
        println!("source {:?} dest {:?}", self.tmp_dir.path().join("trace.dat"), full_filename);
        fs::copy(self.tmp_dir.path().join("trace.dat"), full_filename).map(|_| { () })
        */
        Ok(())
    }
}

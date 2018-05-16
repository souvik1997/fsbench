use std::path::Path;
use std::path::PathBuf;

use super::*;
use fsbench::util::Filesystem;
pub mod create;
pub use self::create::*;
pub mod delete;
pub use self::delete::*;
pub mod rename;
pub use self::rename::*;
pub mod listdir;
pub use self::listdir::*;
use serde::Serialize;
//pub mod varmail;
//pub use self::varmail::*;

pub struct BaseConfiguration<'a> {
    pub filesystem_path: &'a Path,
    pub blktrace: &'a fsbench::blktrace::Blktrace,
    pub output_dir: PathBuf,
}

use fsbench::blktrace::Trace;
use fsbench::statistics::Stats;
pub trait Benchmark<T: Config> {
    fn total(&self) -> Stats;
    fn get_trace<'b>(&'b self) -> &'b Trace;
    fn get_config<'b>(&'b self) -> &'b T;
}

pub trait Config : Serialize {
    fn config_for(fs: &Filesystem) -> Self;
    fn num_files(&self) -> usize;
}

const DEFAULT_DIR_WIDTH: usize = 7;
const DEFAULT_NUM_FILES: usize = 10000;

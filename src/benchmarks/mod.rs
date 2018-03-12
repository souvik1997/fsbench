use std::path::Path;
use std::path::PathBuf;

use super::*;
pub mod create;
pub use self::create::*;
pub mod delete;
pub use self::delete::*;
pub mod rename;
pub use self::rename::*;
pub mod listdir;
pub use self::listdir::*;
pub mod varmail;
pub use self::varmail::*;

pub struct BaseConfiguration<'a> {
    pub filesystem_path: &'a Path,
    pub blktrace: fsbench::blktrace::Blktrace,
    pub output_dir: PathBuf,
}

const DEFAULT_DIR_WIDTH: usize = 7;
const DEFAULT_NUM_FILES: usize = 100000;

use std::path::Path;
use std::path::PathBuf;
use rand::distributions::IndependentSample;

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

pub struct Configuration<'a, R, RV>
where
    R: IndependentSample<f64>,
    RV: IndependentSample<f64>,
{
    pub filesystem_path: &'a Path,
    pub num_files: usize,
    pub dir_width: usize,
    pub file_size_distribution: R,
    pub num_threads: usize,
    pub blktrace: fsbench::blktrace::Blktrace,
    pub output_dir: PathBuf,
    pub varmail_config: VarmailConfig<RV>,
}

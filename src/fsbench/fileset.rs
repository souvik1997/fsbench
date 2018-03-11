use std::path::{Path, PathBuf};

pub struct FileSet {
    num_files: usize,
    base_path: PathBuf,
    dir_width: usize,
}

impl FileSet {
    pub fn new<P: AsRef<Path>>(num_files: usize, base_path: P, dir_width: usize) -> FileSet {
        FileSet {
            num_files: num_files,
            base_path: base_path.as_ref().to_owned(),
            dir_width: dir_width,
        }
    }
}

impl IntoIterator for FileSet {
    type Item = PathBuf;
    type IntoIter = FileSetIterator;
    fn into_iter(self) -> Self::IntoIter {
        let depth = (self.num_files as f64).log(self.dir_width as f64).ceil() as usize;
        let mut state = Vec::new();
        for _ in 0..depth {
            state.push(0);
        }
        FileSetIterator {
            state: state,
            base_path: self.base_path,
            dir_width: self.dir_width,
            counter: 0,
            max: self.num_files,
        }
    }
}

pub struct FileSetIterator {
    state: Vec<usize>,
    base_path: PathBuf,
    dir_width: usize,
    counter: usize,
    max: usize,
}

impl Iterator for FileSetIterator {
    type Item = PathBuf;
    fn next(&mut self) -> Option<Self::Item> {
        if self.counter >= self.max {
            None
        } else {
            for num in self.state.iter_mut() {
                *num += 1;
                if *num >= self.dir_width {
                    *num = 0;
                } else {
                    break;
                }
            }
            let path_buf: PathBuf = self.state.iter().rev().map(|num| num.to_string()).fold(
                self.base_path.clone(),
                |mut path, component| {
                    path.push(&component);
                    path
                },
            );
            self.counter += 1;
            Some(path_buf.clone())
        }
    }
}

use std::path::{Path, PathBuf};
use std::io;
use super::Buffer;
use super::api::BlkIOTrace;

pub struct Trace {
    pub data: Vec<Buffer>,
    pub completed_writes: usize,
    pub completed_reads: usize,
}

unsafe fn as_trace<'a>(b: &'a [u8]) -> &'a BlkIOTrace {
    let s: *const BlkIOTrace = b.as_ptr() as *const _;
    s.as_ref().unwrap()
}

fn count_completed(b: &[u8]) -> (usize /* reads */, usize /* writes */) {
    use std::mem;
    use super::api::{blk_tc_act, Action};
    const COMPLETE_ACTION: u16 = Action::BlkTaComplete as u16;
    const BLK_TC_WRITE: usize = 1 << 1;
    const STEP_SIZE: usize = mem::size_of::<BlkIOTrace>();
    let mut index: usize = 0;
    let mut complete_reads: usize = 0;
    let mut complete_writes: usize = 0;
    while index + STEP_SIZE < b.len() {
        let trace = unsafe { as_trace(&b[index..]) };
        // NOTE: we do not check if the trace is valid
        index += STEP_SIZE;
        index += trace.pdu_len as usize;

        // Check if this event is a read or a write
        let is_write = (trace.action & blk_tc_act(BLK_TC_WRITE)) != 0;

        // Extract the action
        let action = (trace.action & 0xffff) as u16;

        match action {
            // NOTE: blkparse measures `bytes` in KiB by first shifting trace.bytes >> 10, and adding the result. We add the bytes directly
            COMPLETE_ACTION => {
                if is_write {
                    complete_writes += trace.bytes as usize;
                } else {
                    complete_reads += trace.bytes as usize;
                }
            }
            _ => {}
        }
    }
    (complete_reads, complete_writes)
}

impl Trace {
    pub fn new(data: Vec<Buffer>) -> Self {
        let (reads, writes) = data.iter().fold((0, 0), |acc, s| {
            let (nreads, nwrites) = count_completed(&s);
            (acc.0 + nreads, acc.1 + nwrites)
        });
        println!("complete reads: {}, complete writes: {}", reads, writes);
        Self {
            completed_reads: reads,
            completed_writes: writes,
            data: data,
        }
    }

    pub fn num_cpus(&self) -> usize {
        self.data.len()
    }

    pub fn total_bytes(&self) -> usize {
        self.data.iter().fold(0, |acc, s| {
            acc + s.len()
        })
    }

    pub fn export<P: AsRef<Path>, Q: AsRef<Path>>(&self, path: &P, prefix: &Q) -> io::Result<()> {
        use std::io::Write;
        use std::fs::File;
        use std::process::Command;
        use super::super::util::mkdir;
        mkdir(path.as_ref())?;
        for (index, buf) in self.data.iter().enumerate() {
            let mut filename = PathBuf::new();
            filename.set_file_name(prefix.as_ref());
            filename.set_extension(format!("blktrace.{}", index));
            let mut full_filename = PathBuf::new();
            full_filename.push(path);
            full_filename.push(filename);
            match File::create(full_filename) {
                Ok(mut file) => {
                    file.write_all(buf)?;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        let blkparse = Command::new("blkparse")
            .args(&[
                path.as_ref()
                    .join(prefix)
                    .to_str()
                    .expect("failed to convert path to string"),
            ])
            .output();
        match blkparse {
            Ok(blkparse_output) => {
                let mut filename = PathBuf::new();
                filename.set_file_name(prefix.as_ref());
                filename.set_extension("txt");
                let mut full_filename = PathBuf::new();
                full_filename.push(path);
                full_filename.push(filename);
                match File::create(full_filename) {
                    Ok(mut file) => file.write_all(&blkparse_output.stdout),
                    Err(e) => Err(e),
                }
            }
            Err(_) => {
                // Ignore errors caused by running blkparse
                Ok(())
            }
        }
    }
}

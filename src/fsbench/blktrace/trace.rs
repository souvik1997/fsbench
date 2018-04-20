use super::api::BlkIOTrace;
use std::cmp::Ordering;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::collections::*;


pub const SECTOR_SIZE: usize = 512;

#[derive(FromPrimitive, PartialEq, Eq, Debug)]
pub enum Action {
    Other = 0,
    Queue = 1,
    Backmerge = 2,
    Frontmerge = 3,
    GetRQ = 4,
    SleepRQ = 5,
    Requeue = 6,
    Issue = 7,
    Complete = 8,
    Plug = 9,
    UnplugIO = 10,
    UnplugTimer = 11,
    Insert = 12,
    Split = 13,
    Bounce = 14,
    Remap = 15,
    Abort = 16,
    DrvData = 17,
}

bitflags! {
    pub struct Category: u16 {
        const READ	= 1 << 0;	/* reads */
        const WRITE	= 1 << 1;	/* writes */
        const FLUSH	= 1 << 2;	/* flush */
        const SYNC	= 1 << 3;	/* sync IO */
        const QUEUE	= 1 << 4;	/* queueing/merging */
        const REQUEUE	= 1 << 5;	/* requeueing */
        const ISSUE	= 1 << 6;	/* issue */
        const COMPLETE	= 1 << 7;	/* completions */
        const FS	= 1 << 8;	/* fs requests */
        const PC	= 1 << 9;	/* pc requests */
        const NOTIFY	= 1 << 10;	/* special message */
        const AHEAD	= 1 << 11;	/* readahead */
        const META	= 1 << 12;	/* metadata */
        const DISCARD	= 1 << 13;	/* discard requests */
        const DRV_DATA	= 1 << 14;	/* binary per-driver data */
        const FUA	= 1 << 15;	/* fua requests */

        const END	= 1 << 15;	/* we've run out of bits! */
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct EventPDU {
    pub data: Vec<u8>,
}

#[derive(PartialEq, Eq, Debug)]
pub struct Event {
    pub sequence: u32,
    pub time: u64,
    pub sector: u64,
    pub bytes: u32,
    pub action: Action,
    pub category: Category,
    pub pid: u32,
    pub device: u32,
    pub cpu: u32,
    pub error: u16,
    pub pdu: Option<EventPDU>,
}

impl Event {
    fn from_raw(trace: &BlkIOTrace, pdu_data: &[u8]) -> Event {
        use num;
        let pdu = {
            if pdu_data.len() > 0 {
                Some(EventPDU {
                    data: {
                        let mut t = Vec::new();
                        t.extend_from_slice(pdu_data);
                        t
                    },
                })
            } else {
                None
            }
        };
        Event {
            sequence: trace.sequence,
            time: trace.time,
            sector: trace.sector,
            bytes: trace.bytes,
            action: num::FromPrimitive::from_u32(trace.action & 0xffff).expect("invalid action type"),
            category: Category::from_bits_truncate(((trace.action & 0xffff0000) >> 16) as u16),
            pid: trace.pid,
            device: trace.device,
            cpu: trace.cpu,
            error: trace.error,
            pdu: pdu,
        }
    }
}

impl Ord for Event {
    fn cmp(&self, other: &Event) -> Ordering {
        self.time.cmp(&other.time)
    }
}

impl PartialOrd for Event {
    fn partial_cmp(&self, other: &Event) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

unsafe fn as_trace<'a>(b: &'a [u8]) -> &'a BlkIOTrace {
    use std::mem;
    assert!(b.len() == mem::size_of::<BlkIOTrace>());
    let s: *const BlkIOTrace = b.as_ptr() as *const _;
    s.as_ref().unwrap()
}

fn parse(b: &[u8]) -> Vec<Event> {
    use std::mem;
    const STEP_SIZE: usize = mem::size_of::<BlkIOTrace>();
    let mut index: usize = 0;
    let mut events = Vec::new();
    while index + STEP_SIZE < b.len() {
        let trace = unsafe { as_trace(&b[index..index + STEP_SIZE]) };
        index += STEP_SIZE;
        let pdu_data = &b[index..index + trace.pdu_len as usize];
        let event = Event::from_raw(trace, pdu_data);
        events.push(event);
        // NOTE: we do not check if the trace is valid
        index += trace.pdu_len as usize;
    }
    events
}

pub struct Trace {
    data: Vec<Vec<u8>>,
    events: Vec<Event>,
    elapsed: Duration,
}

impl Trace {
    pub fn new(data: Vec<Vec<u8>>, elapsed: Duration) -> Self {
        let mut events = data.iter().map(|d| parse(&d)).fold(Vec::new(), |mut acc, s| { acc.extend(s); acc });
        events.sort();
        Self {
            data: data,
            events: events,
            elapsed: elapsed,
        }
    }

    pub fn num_cpus(&self) -> usize {
        self.data.len()
    }

    pub fn total_bytes(&self) -> usize {
        self.data.iter().fold(0, |acc, s| acc + s.len())
    }

    pub fn export<P: AsRef<Path>, Q: AsRef<Path>>(&self, path: &P, prefix: &Q) -> io::Result<()> {
        use super::super::util::mkdir;
        use std::fs::File;
        use std::io::Write;
        use std::process::Command;
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
        /*
        let blkparse = Command::new("blkparse")
            .args(&[path.as_ref().join(prefix).to_str().expect("failed to convert path to string")])
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
         */
        Ok(())
    }

    pub fn completed_reads<'a>(&'a self) -> usize {
        self.events
            .iter()
            .filter_map(|event| {
                if event.category.contains(Category::READ) && event.action == Action::Complete {
                    Some(event)
                } else {
                    None
                }
            })
            .fold(0, |acc, event| acc + event.bytes as usize)
    }

    pub fn completed_writes(&self) -> usize {
        self.events
            .iter()
            .filter_map(|event| {
                if event.category.contains(Category::WRITE) && event.action == Action::Complete {
                    Some(event)
                } else {
                    None
                }
            })
            .fold(0, |acc, event| acc + event.bytes as usize)
    }

    pub fn total_duration(&self) -> Duration {
        self.elapsed
    }

    pub fn io_duration(&self) -> Duration {
        // Amount of time spent on IO
        // Count the time from queue insertion to completion
        let mut inserted = HashMap::new();
        let mut total_ns: u64 = 0;
        for event in &self.events {
            if event.action == Action::Insert {
                inserted.insert(event.sequence, event.time);
            } else if event.action == Action::Complete {
                // check if we have seen this sequence number
                match inserted.remove(&event.sequence) {
                    Some(timestamp) => {
                        assert!(event.time > timestamp);
                        total_ns += event.time - timestamp;
                    }
                    _ => {}
                }
            }
        }
        Duration::from_nanos(total_ns)
    }

    pub fn num_requests(&self) -> usize {
        let mut sequences = HashSet::new();
        for event in &self.events {
            if !sequences.contains(&event.sequence) {
                sequences.insert(event.sequence);
            }
        }
        sequences.len()
    }
}

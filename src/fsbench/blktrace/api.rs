use super::*;
use std::mem;

/*

The data that is passed to the kernel:

struct blk_user_trace_setup {
  char name[32];			// output
  __u16 act_mask;			// input
  __u32 buf_size;			// input
  __u32 buf_nr;       // input
  __u64 start_lba;
  __u64 end_lba;
  __u32 pid;
};

*/

#[repr(C)]
#[derive(Default)]
pub struct BlkUserTraceSetup {
    pub name: [u8; 32],
    pub act_mask: u16,
    pub buf_size: u32,
    pub buf_nr: u32,
    pub start_lba: u64,
    pub end_lba: u64,
    pub pid: u32,
}

/*

The trace itself

struct blk_io_trace {
  __u32 magic;		/* MAGIC << 8 | version */
  __u32 sequence;		/* event number */
  __u64 time;		/* in nanoseconds */
  __u64 sector;		/* disk offset */
  __u32 bytes;		/* transfer length */
  __u32 action;		/* what happened */
  __u32 pid;		/* who did it */
  __u32 device;		/* device identifier (dev_t) */
  __u32 cpu;		/* on what cpu did it happen */
  __u16 error;		/* completion error */
  __u16 pdu_len;		/* length of data after this trace */
};

*/

#[repr(C)]
pub struct BlkIOTrace {
    pub magic: u32,
    pub sequence: u32,
    pub time: u64,
    pub sector: u64,
    pub bytes: u32,
    pub action: u32,
    pub pid: u32,
    pub device: u32,
    pub cpu: u32,
    pub error: u16,
    pub pdu_len: u16,
}

#[allow(dead_code)]
#[repr(u16)]
pub enum Action {
    BlkTaQueue = 1,   /* queued */
    BlkTaBackMerge,   /* back merged to existing rq */
    BlkTaFrontMerge,  /* front merge to existing rq */
    BlkTaGetRQ,       /* allocated new request */
    BlkTaSleepRQ,     /* sleeping on rq allocation */
    BlkTaRequeue,     /* request requeued */
    BlkTaIssue,       /* sent to driver */
    BlkTaComplete,    /* completed by driver */
    BlkTaPlug,        /* queue was plugged */
    BlkTaUnplugIO,    /* queue was unplugged by io */
    BlkTaUnplugTimer, /* queue was unplugged by timer */
    BlkTaInsert,      /* insert request */
    BlkTaSplit,       /* bio was split */
    BlkTaBounce,      /* bio was bounced */
    BlkTaRemap,       /* bio was remapped */
    BlkTaAbort,       /* request aborted */
    BlkTaDrvData,     /* binary driver data */
}

pub fn blk_tc_act(category: usize) -> u32 {
    const BLK_TC_SHIFT: usize = 16;
    (category << BLK_TC_SHIFT) as u32
}

pub fn setup(fd: RawFd, obj: *mut BlkUserTraceSetup) -> i32 {
    use super::nix::*;
    const BLKTRACESETUP_MAGIC1: u8 = 0x12;
    const BLKTRACESETUP_MAGIC2: u8 = 115;
    unsafe {
        libc::ioctl(
            fd,
            iorw!(
                BLKTRACESETUP_MAGIC1,
                BLKTRACESETUP_MAGIC2,
                mem::size_of::<BlkUserTraceSetup>()
            ),
            obj,
        )
    }
}

pub fn start(fd: RawFd) -> i32 {
    use super::nix::*;
    const BLKTRACESTART_MAGIC1: u8 = 0x12;
    const BLKTRACESTART_MAGIC2: u8 = 116;
    unsafe { libc::ioctl(fd, io!(BLKTRACESTART_MAGIC1, BLKTRACESTART_MAGIC2)) }
}

pub fn stop(fd: RawFd) -> i32 {
    use super::nix::*;
    const BLKTRACESTOP_MAGIC1: u8 = 0x12;
    const BLKTRACESTOP_MAGIC2: u8 = 117;
    unsafe { libc::ioctl(fd, io!(BLKTRACESTOP_MAGIC1, BLKTRACESTOP_MAGIC2)) }
}

pub fn teardown(fd: RawFd) -> i32 {
    use super::nix::*;
    const BLKTRACETEARDOWN_MAGIC1: u8 = 0x12;
    const BLKTRACETEARDOWN_MAGIC2: u8 = 118;
    unsafe { libc::ioctl(fd, io!(BLKTRACETEARDOWN_MAGIC1, BLKTRACETEARDOWN_MAGIC2)) }
}

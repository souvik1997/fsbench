pub struct Mount<'a> {
    filesystem_path: &'a str,
}

impl<'a> Mount<'a> {
    pub fn new(device: &'a str, filesystem_path: &'a str) -> Self {
        use std::process::Command;
        // Mount the device at the mountpoint using the `mount` command
        // NOTE: we could use mount(2), but that doesn't auto-detect the filesystem
        // which means we would have to try each filesystem that the kernel supports.
        // mount returns with exit code 0 if it succeeds.
        if !Command::new("mount")
            .args(&[device, filesystem_path])
            .status()
            .expect("failed to run `mount`")
            .success()
        {
            panic!("failed to mount {} on {:?}", device, filesystem_path);
        }
        Self {
            filesystem_path: filesystem_path,
        }
    }
}

impl<'a> Drop for Mount<'a> {
    fn drop(&mut self) {
        use std::process::Command;
        if !Command::new("umount")
            .args(&[self.filesystem_path])
            .status()
            .expect("failed to run `mount`")
            .success()
        {
            panic!("failed to unmount {:?}", self.filesystem_path);
        }
    }
}

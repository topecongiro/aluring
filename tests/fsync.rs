use std::os::unix::prelude::AsRawFd;

use aluring::{
    buf::UringBuf,
    result::IoResult,
    sqe::{FdatasyncData, FsyncData, Sqe, WriteData},
    Uring,
};

#[test]
fn test_single_fsync() {
    let ring = Uring::new(8).unwrap();
    let f = tempfile::NamedTempFile::new().unwrap();
    let handle = ring
        .prepare_fsync(Sqe::new(FsyncData { fd: f.as_raw_fd() }))
        .unwrap();
    ring.submit().unwrap();
    let res = handle.wait().unwrap();
    assert!(res.as_io_result().is_ok());
}

#[test]
fn test_barrier_fsync() {
    let ring = Uring::new(8).unwrap();
    let f = tempfile::NamedTempFile::new().unwrap();
    let bufs = vec![vec![0; 4096]; 4];
    let mut offset = 0;
    let mut handles = vec![];
    for buf in bufs {
        handles.push(
            ring.prepare_write(Sqe::new(WriteData {
                fd: f.as_raw_fd(),
                buf: UringBuf::Vec(buf),
                offset,
            }))
            .unwrap(),
        );
        offset += 4096;
    }
    let fdatasync_handle = ring
        .prepare_fdatasync(Sqe::new(FdatasyncData { fd: f.as_raw_fd() }).drain())
        .unwrap();
    let submitted = ring.submit().unwrap();
    assert_eq!(submitted, 5);

    assert!(fdatasync_handle.wait().unwrap().as_io_result().is_ok());
    for h in handles {
        assert!(h.observed());
        assert!(h.wait().unwrap().as_io_result().is_ok());
    }
}

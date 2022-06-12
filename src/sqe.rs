use std::{os::unix::io::RawFd, ptr::NonNull};

use uring_sys2::*;

use crate::UringBuf;

pub(crate) trait UringSqe: Into<UringOperationKind> {
    fn prepare(&mut self, sqe: NonNull<io_uring_sqe>);
}

pub struct SqeBuilder {
    flag: u32,
}

impl SqeBuilder {
    pub fn new() -> SqeBuilder {
        SqeBuilder { flag: 0 }
    }

    pub fn link(&mut self) -> &mut SqeBuilder {
        self.flag |= IOSQE_IO_LINK;
        self
    }

    pub fn read(self, fd: RawFd, buf: UringBuf, offset: u64) -> ReadSqe {
        ReadSqe {
            flag: self.flag,
            data: ReadData { fd, buf, offset },
        }
    }
}

/// SQE for `read`.
pub struct ReadSqe {
    flag: u32,
    data: ReadData,
}

pub(crate) struct ReadData {
    pub(crate) fd: RawFd,
    pub(crate) buf: UringBuf,
    pub(crate) offset: u64,
}

impl Into<UringOperationKind> for ReadSqe {
    fn into(self) -> UringOperationKind {
        UringOperationKind::Read(self.data)
    }
}

impl UringSqe for ReadSqe {
    fn prepare(&mut self, sqe: NonNull<io_uring_sqe>) {
        unsafe {
            io_uring_prep_read(
                sqe.as_ptr(),
                self.data.fd,
                self.data.buf.as_mut_ptr() as *mut _,
                self.data.buf.len() as u32,
                self.data.offset,
            );
            io_uring_sqe_set_flags(sqe.as_ptr(), self.flag);
        }
    }
}

pub struct WriteSqe {
    flag: u32,
    data: WriteData,
}

pub(crate) struct WriteData {
    pub(crate) fd: RawFd,
    pub(crate) buf: UringBuf,
    pub(crate) offset: u64,
}

impl Into<UringOperationKind> for WriteSqe {
    fn into(self) -> UringOperationKind {
        UringOperationKind::Write(self.data)
    }
}

impl UringSqe for WriteSqe {
    fn prepare(&mut self, sqe: NonNull<io_uring_sqe>) {
        unsafe {
            io_uring_prep_write(
                sqe.as_ptr(),
                self.data.fd,
                self.data.buf.as_mut_ptr() as *mut _,
                self.data.buf.len() as u32,
                self.data.offset,
            );
            io_uring_sqe_set_flags(sqe.as_ptr(), self.flag)
        }
    }
}

pub struct FsyncSqe {
    flag: u32,
    data: FsyncData,
}

impl Into<UringOperationKind> for FsyncSqe {
    fn into(self) -> UringOperationKind {
        UringOperationKind::Fsync(self.data)
    }
}

impl UringSqe for FsyncSqe {
    fn prepare(&mut self, sqe: NonNull<io_uring_sqe>) {
        unsafe {
            io_uring_prep_fsync(sqe.as_ptr(), self.data.fd, 0);
            io_uring_sqe_set_flags(sqe.as_ptr(), self.flag);
        }
    }
}

pub struct FdatasyncSqe {
    flag: u32,
    data: FsyncData,
}

impl Into<UringOperationKind> for FdatasyncSqe {
    fn into(self) -> UringOperationKind {
        UringOperationKind::Fdatasync(self.data)
    }
}

impl UringSqe for FdatasyncSqe {
    fn prepare(&mut self, sqe: NonNull<io_uring_sqe>) {
        unsafe {
            io_uring_prep_fsync(sqe.as_ptr(), self.data.fd, IORING_FSYNC_DATASYNC);
            io_uring_sqe_set_flags(sqe.as_ptr(), self.flag);
        }
    }
}

pub(crate) struct FsyncData {
    pub(crate) fd: RawFd,
}

pub struct MadviseSqe {
    flag: u32,
    data: MadviseData,
}

impl Into<UringOperationKind> for MadviseSqe {
    fn into(self) -> UringOperationKind {
        UringOperationKind::Madvise(self.data)
    }
}

impl UringSqe for MadviseSqe {
    fn prepare(&mut self, sqe: NonNull<io_uring_sqe>) {
        unsafe {
            io_uring_prep_madvise(
                sqe.as_ptr(),
                self.data.buf.as_mut_ptr() as *mut _,
                self.data.length,
                self.data.advise as i32,
            );
            io_uring_sqe_set_flags(sqe.as_ptr(), self.flag);
        }
    }
}

pub(crate) struct MadviseData {
    pub(crate) buf: UringBuf,
    pub(crate) length: i64,
    pub(crate) advise: Madvise,
}

/// The advise to `madvise(2)`.
// FIXME: add more variants.
#[repr(i32)]
#[derive(Debug, Copy, Clone)]
pub enum Madvise {
    Normal = libc::MADV_NORMAL,
    DontNeed = libc::MADV_DONTNEED,
}

pub(crate) enum UringOperationKind {
    /// Asynchronous `read(2)`.
    ///
    /// Equivalent to `io_uring_prep_read`.
    Read(ReadData),
    /// Asynchronous `write(2).
    ///
    /// Equivalent to `io_uring_prep_write`
    Write(WriteData),
    /// Asynchronous `fsync(2)`.
    ///
    /// Equivalent to `io_uring_prep_fsync`
    Fsync(FsyncData),
    /// Asynchronous `fdatasync(2)`.
    ///
    /// Equivalent to `io_uring_prep_fsync` with `IORING_FSYNC_DATASYNC`.
    Fdatasync(FsyncData),
    /// Asynchronous `madvise(2)`.
    ///
    /// Equivalent to `io_uring_prep_madvise`.
    Madvise(MadviseData),
}

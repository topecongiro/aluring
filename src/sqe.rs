//! Submission queue entry of `io_uring`.
use std::{os::unix::io::RawFd, ptr::NonNull};

use uring_sys2::*;

use crate::{
    handle::Handler, FdatasyncHandle, FsyncHandle, MadviseHandle, ReadHandle, UringBuf, WriteHandle,
};

pub(crate) trait UringSqe<'a>: Into<UringOperationKind> {
    type Handle: Handler<'a>;

    fn prepare(&mut self, sqe: NonNull<io_uring_sqe>);
}

/// Submission queue entry (SQE) of `io_uring`.
pub struct Sqe<T> {
    pub(crate) flag: u32,
    pub(crate) data: T,
}

/// Data type for io_uring operations.
pub trait UringData {}

impl Sqe<ReadData> {
    /// Creates a new `Sqe` for `read(2)`.
    pub fn read(fd: RawFd, buf: UringBuf, offset: u64) -> Sqe<ReadData> {
        Sqe {
            flag: 0,
            data: ReadData { fd, buf, offset },
        }
    }
}

impl Sqe<WriteData> {
    /// Creates a new `Sqe` for `write(2)`.
    pub fn write(fd: RawFd, buf: UringBuf, offset: u64) -> Sqe<WriteData> {
        Sqe {
            flag: 0,
            data: WriteData { fd, buf, offset },
        }
    }
}

impl Sqe<MadviseData> {
    /// Creates a new `Sqe` for `madvise(2)`.
    pub fn madvise(buf: UringBuf, advise: Madvise) -> Sqe<MadviseData> {
        Sqe {
            flag: 0,
            data: MadviseData { buf, advise },
        }
    }
}

impl Sqe<FsyncData> {
    /// Creates a new `Sqe` for `fsync(2)`.
    pub fn fsync(fd: RawFd) -> Sqe<FsyncData> {
        Sqe {
            flag: 0,
            data: FsyncData { fd },
        }
    }
}

impl Sqe<FdatasyncData> {
    /// Creates a new `Sqe` for `fdatasync(2)`.
    pub fn fdatasync(fd: RawFd) -> Sqe<FdatasyncData> {
        Sqe {
            flag: 0,
            data: FdatasyncData { fd },
        }
    }
}

impl<T: UringData> Sqe<T> {
    /// Creates a new `Sqe`.
    pub fn new(data: T) -> Sqe<T> {
        Sqe { flag: 0, data }
    }

    /// Enables drain.
    pub fn drain(mut self) -> Sqe<T> {
        self.flag |= IOSQE_IO_DRAIN;
        self
    }

    /// Enables link.
    pub fn link(mut self) -> Sqe<T> {
        self.flag |= IOSQE_IO_LINK;
        self
    }

    /// Enables hard link.
    pub fn hard_link(mut self) -> Sqe<T> {
        self.flag |= IOSQE_IO_HARDLINK;
        self
    }

    /// Enables skip cqe on success.
    pub fn skip_cqe_on_success(mut self) -> Sqe<T> {
        self.flag |= IOSQE_CQE_SKIP_SUCCESS;
        self
    }
}

/// Input for asynchronous `read(2)`.
pub struct ReadData {
    pub fd: RawFd,
    pub buf: UringBuf,
    pub offset: u64,
}
impl UringData for ReadData {}

impl Into<UringOperationKind> for Sqe<ReadData> {
    fn into(self) -> UringOperationKind {
        UringOperationKind::Read(self.data)
    }
}

impl<'a> UringSqe<'a> for Sqe<ReadData> {
    type Handle = ReadHandle<'a>;

    fn prepare(&mut self, sqe: NonNull<io_uring_sqe>) {
        unsafe {
            io_uring_prep_read(
                sqe.as_ptr(),
                self.data.fd,
                self.data.buf.as_mut_ptr() as *mut _,
                self.data.buf.len() as u32,
                self.data.offset,
            );
        }
    }
}

/// Input for asynchronous `write(2)`.
pub struct WriteData {
    pub fd: RawFd,
    pub buf: UringBuf,
    pub offset: u64,
}
impl UringData for WriteData {}

impl Into<UringOperationKind> for Sqe<WriteData> {
    fn into(self) -> UringOperationKind {
        UringOperationKind::Write(self.data)
    }
}

impl<'a> UringSqe<'a> for Sqe<WriteData> {
    type Handle = WriteHandle<'a>;
    fn prepare(&mut self, sqe: NonNull<io_uring_sqe>) {
        unsafe {
            io_uring_prep_write(
                sqe.as_ptr(),
                self.data.fd,
                self.data.buf.as_mut_ptr() as *mut _,
                self.data.buf.len() as u32,
                self.data.offset,
            );
        }
    }
}

/// Input for asynchronous `fsync(2)`.
pub struct FsyncData {
    pub fd: RawFd,
}
impl UringData for FsyncData {}

impl Into<UringOperationKind> for Sqe<FsyncData> {
    fn into(self) -> UringOperationKind {
        UringOperationKind::Fsync(self.data)
    }
}

impl<'a> UringSqe<'a> for Sqe<FsyncData> {
    type Handle = FsyncHandle<'a>;

    fn prepare(&mut self, sqe: NonNull<io_uring_sqe>) {
        unsafe {
            io_uring_prep_fsync(sqe.as_ptr(), self.data.fd, 0);
        }
    }
}

/// Input for asynchronous `fdatasync(2)`.
pub struct FdatasyncData {
    pub fd: RawFd,
}
impl UringData for FdatasyncData {}

impl Into<UringOperationKind> for Sqe<FdatasyncData> {
    fn into(self) -> UringOperationKind {
        UringOperationKind::Fdatasync(self.data)
    }
}

impl<'a> UringSqe<'a> for Sqe<FdatasyncData> {
    type Handle = FdatasyncHandle<'a>;

    fn prepare(&mut self, sqe: NonNull<io_uring_sqe>) {
        unsafe {
            io_uring_prep_fsync(sqe.as_ptr(), self.data.fd, IORING_FSYNC_DATASYNC);
        }
    }
}

/// Input for asynchronous `madvise(2)`.
pub struct MadviseData {
    pub buf: UringBuf,
    pub advise: Madvise,
}
impl UringData for MadviseData {}

impl Into<UringOperationKind> for Sqe<MadviseData> {
    fn into(self) -> UringOperationKind {
        UringOperationKind::Madvise(self.data)
    }
}

impl<'a> UringSqe<'a> for Sqe<MadviseData> {
    type Handle = MadviseHandle<'a>;

    fn prepare(&mut self, sqe: NonNull<io_uring_sqe>) {
        unsafe {
            io_uring_prep_madvise(
                sqe.as_ptr(),
                self.data.buf.as_mut_ptr() as *mut _,
                self.data.buf.len() as i64,
                self.data.advise as i32,
            );
        }
    }
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
    Fdatasync(FdatasyncData),
    /// Asynchronous `madvise(2)`.
    ///
    /// Equivalent to `io_uring_prep_madvise`.
    Madvise(MadviseData),
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_sqe() {
        let _sqe = Sqe::read(0, UringBuf::Vec(vec![]), 0);
        let _sqe = Sqe::write(0, UringBuf::Vec(vec![]), 0);
        let _sqe = Sqe::madvise(UringBuf::Vec(vec![]), Madvise::DontNeed);
        let _sqe = Sqe::fsync(0);
        let _sqe = Sqe::fdatasync(0);
    }
}

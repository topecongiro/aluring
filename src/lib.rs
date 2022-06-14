//! `liburing` wrapper library without `async`.
//!
//! ```rust,no_run
//! # use std::fs::File;
//! use aluring::{Uring, sqe::Sqe, buf::UringBuf, sqe::ReadData};
//!
//! # fn main() -> anyhow::Result<()> {
//! # use std::os::unix::io::AsRawFd;
//! let uring = Uring::new(128)?;
//! let f = File::open("example.txt")?;
//! let mut handle = uring.prepare_read(Sqe::new(ReadData {
//!     fd: f.as_raw_fd(),
//!     buf: UringBuf::Vec(vec![0; 1024]),
//!     offset: 0,
//! }))?;
//! uring.submit()?;
//! handle.wait()?;
//! # Ok(())
//! # }
//! ```
use std::{
    cell::{RefCell, RefMut, UnsafeCell},
    collections::{hash_map::Entry, HashMap},
    io,
    mem::MaybeUninit,
    ptr,
    ptr::NonNull,
};

use thiserror::Error;
use uring_sys2::*;

use crate::{
    buf::UringBuf,
    handle::{FdatasyncHandle, FsyncHandle, Handler, MadviseHandle, ReadHandle, WriteHandle},
    sqe::{
        FdatasyncData, FsyncData, MadviseData, ReadData, Sqe, UringOperationKind, UringSqe,
        WriteData,
    },
};

pub mod buf;
pub mod handle;
pub mod result;
pub mod sqe;

/// liburing interface without `async`.
pub struct Uring {
    ring: UnsafeCell<io_uring>,
    state: RefCell<UringState>,
}

/// Internal state.
struct UringState {
    id_gen: u64,
    /// Keeps track of ongoing/completed io_uring operations.
    map: HashMap<u64, UringOperation>,
    submitted_count: usize,
}

impl UringState {
    fn new(entries: usize) -> Self {
        UringState {
            id_gen: 0,
            map: HashMap::with_capacity(entries),
            submitted_count: 0,
        }
    }
}

struct UringContext<'a> {
    state: RefMut<'a, UringState>,
}

/// Errors from [`Uring`](Uring).
#[derive(Debug, Error)]
pub enum Error {
    #[error("io_uring_queue_init({1}, 0) failed")]
    InitError(#[source] io::Error, usize),
    #[error("io_uring_get_sqe failed")]
    GetSqeError,
    #[error("io_uring_submit failed")]
    SubmitError(#[source] io::Error),
    #[error("io_uring_wait_cqe failed")]
    WaitCqeError(#[source] io::Error),
    #[error("internal error: {0}")]
    InternalError(String), // FIXME: add internal errors instead of raw strings.
}

pub type Result<T> = std::result::Result<T, Error>;

impl Uring {
    /// Creates a new `Uring`.
    pub fn new(entries: usize) -> Result<Self> {
        let mut ring = MaybeUninit::uninit();
        let ring = unsafe {
            let ret = io_uring_queue_init(entries as u32, ring.as_mut_ptr(), 0);
            if ret < 0 {
                return Err(Error::InitError(
                    io::Error::from_raw_os_error(-ret),
                    entries,
                ));
            }
            UnsafeCell::new(ring.assume_init())
        };

        Ok(Uring {
            ring,
            state: RefCell::new(UringState::new(entries)),
        })
    }

    /// Submits pending SQEs.
    ///
    /// Returns the number of submitted entries.
    pub fn submit(&self) -> Result<usize> {
        self.submit_with_context(&mut self.context())
    }

    /// Prepares for asynchronous `read(2)`.
    ///
    /// Equivalent to `io_uring_prep_read`.
    pub fn prepare_read(&self, entry: Sqe<ReadData>) -> Result<ReadHandle> {
        self.prepare(&mut self.context(), entry)
    }

    /// Prepares for asynchronous `write(2)`.
    ///
    /// Equivalent to `io_uring_prep_write`.
    pub fn prepare_write(&self, entry: Sqe<WriteData>) -> Result<WriteHandle> {
        self.prepare(&mut self.context(), entry)
    }

    pub fn prepare_fsync(&self, entry: Sqe<FsyncData>) -> Result<FsyncHandle> {
        self.prepare(&mut self.context(), entry)
    }

    pub fn prepare_fdatasync(&self, entry: Sqe<FdatasyncData>) -> Result<FdatasyncHandle> {
        self.prepare(&mut self.context(), entry)
    }

    pub fn prepare_madvise(&self, entry: Sqe<MadviseData>) -> Result<MadviseHandle> {
        self.prepare(&mut self.context(), entry)
    }

    fn context(&self) -> UringContext {
        UringContext {
            state: self.state.borrow_mut(),
        }
    }

    fn wait_single_cqe(&self, context: &mut UringContext) -> Result<Option<u64>> {
        if context.state.submitted_count == 0 {
            return Ok(None);
        }

        let mut cqe = ptr::null_mut();
        unsafe {
            let ret = io_uring_wait_cqe(self.ring.get(), &mut cqe);
            if ret == 0 {
                self.handle_cqe(context, NonNull::new_unchecked(cqe))
                    .map(Some)
            } else {
                Err(Error::WaitCqeError(io::Error::from_raw_os_error(-ret)))
            }
        }
    }

    fn handle_cqe(&self, context: &mut UringContext, cqe: NonNull<io_uring_cqe>) -> Result<u64> {
        context.state.submitted_count -= 1;

        unsafe {
            let res = cqe.as_ref().res;
            let id = io_uring_cqe_get_data64(cqe.as_ptr());
            io_uring_cqe_seen(self.ring.get(), cqe.as_ptr());
            assert_ne!(id, 0);

            match context.state.map.entry(id) {
                Entry::Vacant(_) => Err(Error::InternalError(format!(
                    "no entry in the state map for id {}",
                    id
                ))),
                Entry::Occupied(mut op) => {
                    match op.get().status {
                        OperationStatus::Cancelled => {
                            op.remove();
                        }
                        _ => op.get_mut().status = OperationStatus::Completed(res),
                    }
                    Ok(id)
                }
            }
        }
    }

    fn wait_for(&self, context: &mut UringContext, id: u64) -> Result<()> {
        while let Some(new_id) = self.wait_single_cqe(context)? {
            if id == new_id {
                return Ok(());
            }
        }

        self.submit_with_context(context)?;

        while let Some(new_id) = self.wait_single_cqe(context)? {
            if id == new_id {
                return Ok(());
            }
        }

        Err(Error::InternalError(format!(
            "wait_for({}) could not find the operation with the given id",
            id
        )))
    }

    fn sqe(&self, context: &mut UringContext) -> Result<NonNull<io_uring_sqe>> {
        unsafe {
            let sqe = io_uring_get_sqe(self.ring.get());
            match NonNull::new(sqe) {
                Some(sqe) => Ok(sqe),
                None => {
                    self.submit_with_context(context)?;
                    NonNull::new(io_uring_get_sqe(self.ring.get())).ok_or(Error::GetSqeError)
                }
            }
        }
    }

    fn submit_with_context(&self, context: &mut UringContext) -> Result<usize> {
        let submitted = unsafe {
            let ret = io_uring_submit(self.ring.get());
            if ret < 0 {
                return if ret == -libc::EBUSY {
                    self.submit_with_context(context)
                } else {
                    Err(Error::SubmitError(io::Error::from_raw_os_error(-ret)))
                };
            }
            ret as usize
        };

        context.state.submitted_count += submitted;
        Ok(submitted)
    }

    fn prepare<'a, T>(
        &'a self,
        context: &mut UringContext,
        mut uring_sqe: Sqe<T>,
    ) -> Result<<Sqe<T> as UringSqe<'a>>::Handle>
    where
        Sqe<T>: UringSqe<'a>,
    {
        let sqe = self.sqe(context)?;
        context.state.id_gen += 1;
        let id = context.state.id_gen;

        uring_sqe.prepare(sqe);
        unsafe {
            io_uring_sqe_set_flags(sqe.as_ptr(), uring_sqe.flag);
            io_uring_sqe_set_data64(sqe.as_ptr(), id);
        }

        context.state.map.insert(
            id,
            UringOperation {
                status: OperationStatus::Ongoing,
                kind: uring_sqe.into(),
            },
        );

        Ok(<Sqe<T> as UringSqe<'a>>::Handle::new(id, self))
    }
}

struct UringOperation {
    status: OperationStatus,
    kind: UringOperationKind,
}

enum OperationStatus {
    /// Pending or submitted and hasn't been observed in the CQ.
    Ongoing,
    /// Observed in the CQ.
    Completed(i32),
    /// Cancelled; the user is no longer interested in the result.
    Cancelled,
}

impl Drop for Uring {
    fn drop(&mut self) {
        let mut context = self.context();
        while let Ok(Some(_id)) = self.wait_single_cqe(&mut context) {}
        unsafe { io_uring_queue_exit(self.ring.get()) }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::result::{BufIoResult, IoResult};
    use std::{io::Write, os::unix::io::AsRawFd};

    #[test]
    fn test_read() {
        let ring = Uring::new(64).unwrap();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        let s = "hello, world\n";
        f.write_all(s.as_bytes()).unwrap();

        let mut handles = vec![];
        for _ in 0..256 {
            let h = ring
                .prepare_read(Sqe::new(ReadData {
                    fd: f.as_raw_fd(),
                    buf: UringBuf::Vec(vec![0; 128]),
                    offset: 0,
                }))
                .unwrap();
            handles.push(h);
        }

        for h in handles {
            let result = h.wait().unwrap();
            let len = result.as_io_result().unwrap();
            let buf = result.into_buf();
            assert_eq!(&buf.as_slice()[..len], s.as_bytes());
        }
    }
}

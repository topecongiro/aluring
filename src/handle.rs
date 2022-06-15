//! Handle for an ongoing or completed io_uring operation.
use std::collections::hash_map::Entry;

use crate::{result::*, OperationStatus, Result, Uring, UringOperation, UringOperationKind};

pub(crate) trait Handler<'a>: Into<UringHandle<'a>> {
    type Output;

    fn new(id: u64, ring: &'a Uring) -> Self;
}

macro_rules! define_handle {
    ($([$var:ident, $h:ident, $result:ident, $doc:expr],)*) => {
        /// Generalized `Uring` operation handler.
        pub enum UringHandle<'a> {
            $(
                #[doc = $doc]
                $var($h<'a>),
            )*
        }
        $(
            #[doc = $doc]
            pub struct $h<'a>(Handle<'a>);
            impl<'a> $h<'a> {
                /// Waits for the asynchronous operation and returns its handle.
                pub fn wait(self) -> Result<$result> {
                    self.0.wait()?.try_into()
                }

                /// Returns true if the result is already observed.
                pub fn observed(&self) -> bool {
                    self.0.observed()
                }
            }
            impl<'a> Into<UringHandle<'a>> for $h<'a> {
                fn into(self) -> UringHandle<'a> {
                    UringHandle::$var(self)
                }
            }
            impl<'a> Handler<'a> for $h<'a> {
                type Output = $result;
                fn new(id: u64, ring: &'a Uring) -> Self {
                    $h(Handle::new(id, ring))
                }
            }
        )*
    }
}

define_handle!(
    [Read, ReadHandle, ReadResult, "Handler for `read`."],
    [Write, WriteHandle, WriteResult, "Handler for `write`."],
    [Fsync, FsyncHandle, FsyncResult, "Handler for `fsync`."],
    [
        Fdatasync,
        FdatasyncHandle,
        FdatasyncResult,
        "Handler for `fdatasync`."
    ],
    [
        Madvise,
        MadviseHandle,
        MadviseResult,
        "Handler for `madvise`."
    ],
);

/// General handle for `Uring` operations.
pub(crate) struct Handle<'a> {
    id: u64,
    ring: &'a Uring,
}

impl<'a> Handle<'a> {
    pub(crate) fn new(id: u64, ring: &'a Uring) -> Handle<'a> {
        Handle { id, ring }
    }

    fn observed(&self) -> bool {
        self.ring
            .state
            .borrow()
            .map
            .get(&self.id)
            .map(|e| match e.status {
                OperationStatus::Completed(_) => true,
                _ => false,
            })
            .unwrap_or(false)
    }

    fn wait(self) -> Result<(i32, UringOperationKind)> {
        let mut context = self.ring.context();
        match context.state.map.entry(self.id) {
            Entry::Occupied(op) => match op.get() {
                UringOperation {
                    status: OperationStatus::Completed(res),
                    ..
                } => {
                    let res = *res;
                    let op = op.remove();
                    Ok((res, op.kind))
                }
                _ => {
                    self.ring.wait_for(&mut context, self.id)?;
                    match context.state.map.remove(&self.id) {
                        Some(UringOperation {
                            kind,
                            status: OperationStatus::Completed(res),
                        }) => Ok((res, kind)),
                        _ => unreachable!(
                            "no completed entry for {} in state after `wait_for`",
                            self.id
                        ),
                    }
                }
            },
            _ => unreachable!("no entry for {} in state", self.id),
        }
    }
}

impl<'a> Drop for Handle<'a> {
    fn drop(&mut self) {
        let mut state = self.ring.state.borrow_mut();
        if let Entry::Occupied(mut op) = state.map.entry(self.id) {
            // Dropped before waiting on this handle; tell the Uring to ignore the result.
            match op.get().status {
                OperationStatus::Completed(_) => {
                    op.remove();
                }
                _ => op.get_mut().status = OperationStatus::Cancelled,
            }
        }
    }
}

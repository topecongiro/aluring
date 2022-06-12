use std::collections::hash_map::Entry;

use crate::{result::*, Error, OperationStatus, Result, Uring, UringOperationKind};

pub trait Handler<'a>: Into<UringHandle<'a>> {
    type Output;

    fn wait(self) -> Result<Self::Output>;
}

macro_rules! define_handle {
    ($([$var:ident, $h:ident, $result:ident, $doc:expr],)*) => {
        /// Generalized `Uring` operation handler.
        pub enum UringHandle<'a> {
            $(
                $var($h<'a>),
            )*
        }
        $(
            #[doc = $doc]
            pub struct $h<'a>(Handle<'a>);
            impl<'a> Into<UringHandle<'a>> for $h<'a> {
                fn into(self) -> UringHandle<'a> {
                    UringHandle::$var(self)
                }
            }
            impl<'a> Handler<'a> for $h<'a> {
                type Output = $result;
                fn wait(self) -> Result<$result> {
                    self.0.wait()?.try_into()
                }
            }
            impl<'a> $h<'a> {
                pub(crate) fn new(handler: Handle<'a>) -> Self {
                    $h(handler)
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

    fn wait(self) -> Result<(i32, UringOperationKind)> {
        let mut context = self.ring.context();
        self.ring.wait_for(&mut context, self.id)?;
        let op = context
            .state
            .map
            .remove(&self.id)
            .expect("key must be present");
        if let OperationStatus::Completed(res) = op.status {
            Ok((res, op.kind))
        } else {
            Err(Error::InternalError(String::from(
                "trying to convert to result when operation is not finished",
            )))
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

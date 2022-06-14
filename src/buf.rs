//! Buffer for `io_uring`.

/// Buffer for `io_uring`.
pub enum UringBuf {
    /// `Vec` from the standard library.
    Vec(Vec<u8>),
    /// Unmanaged memory region.
    ///
    /// User of this library must ensure that the pointed memory region is live
    /// until the operation completes.
    Raw { ptr: *mut u8, len: usize },
}

impl UringBuf {
    pub(crate) fn as_mut_ptr(&mut self) -> *mut u8 {
        match self {
            UringBuf::Vec(ref mut v) => v.as_mut_ptr(),
            UringBuf::Raw { ptr, .. } => *ptr,
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            UringBuf::Vec(ref v) => v.as_ref(),
            UringBuf::Raw { ptr, len } => unsafe { std::slice::from_raw_parts(*ptr, *len) },
        }
    }

    pub fn len(&self) -> usize {
        match self {
            UringBuf::Vec(ref v) => v.len(),
            UringBuf::Raw { len, .. } => *len,
        }
    }
}

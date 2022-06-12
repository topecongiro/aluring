use std::io;

use crate::{buf::UringBuf, sqe::*, Error};

pub trait IoResult: Into<UringResult> {
    type Output;

    fn as_io_result(&self) -> io::Result<Self::Output>;
}

pub trait BufIoResult: IoResult {
    fn into_buf(self) -> UringBuf;
}

pub enum UringResult {
    Read(ReadResult),
    Write(WriteResult),
    Fsync(FsyncResult),
    Fdatasync(FdatasyncResult),
    Madvise(MadviseResult),
}

macro_rules! try_io {
    ($res:expr, $e:expr) => {
        if $res < 0 {
            Err(io::Error::from_raw_os_error(-$res))
        } else {
            Ok($e)
        }
    };
}

macro_rules! define_buf_io_result {
    ($result:ident, $variant:ident, $data:ident) => {
        pub struct $result {
            buf: UringBuf,
            res: i32,
        }

        impl $result {
            pub(crate) fn new(buf: UringBuf, res: i32) -> $result {
                $result { buf, res }
            }
        }

        impl IoResult for $result {
            type Output = usize;

            fn as_io_result(&self) -> io::Result<Self::Output> {
                try_io!(self.res, self.res as usize)
            }
        }

        impl BufIoResult for $result {
            fn into_buf(self) -> UringBuf {
                self.buf
            }
        }

        impl Into<UringResult> for $result {
            fn into(self) -> UringResult {
                UringResult::$variant(self)
            }
        }

        impl TryInto<$result> for (i32, UringOperationKind) {
            type Error = Error;

            fn try_into(self) -> Result<$result, Self::Error> {
                match self {
                    (res, UringOperationKind::$variant($data { buf, .. })) => {
                        Ok($result::new(buf, res))
                    }
                    _ => Err(Error::InternalError(String::from(concat!(
                        "invalid conversion from UringOperationKind to ",
                        stringify!($result)
                    )))),
                }
            }
        }
    };
}

macro_rules! define_empty_io_result {
    ($result:ident, $variant:ident, $data:ident) => {
        pub struct $result {
            res: i32,
        }

        impl $result {
            pub(crate) fn new(res: i32) -> $result {
                $result { res }
            }
        }

        impl Into<UringResult> for $result {
            fn into(self) -> UringResult {
                UringResult::$variant(self)
            }
        }

        impl IoResult for $result {
            type Output = ();

            fn as_io_result(&self) -> io::Result<Self::Output> {
                try_io!(self.res, ())
            }
        }

        impl TryInto<$result> for (i32, UringOperationKind) {
            type Error = Error;

            fn try_into(self) -> Result<$result, Self::Error> {
                match self {
                    (res, UringOperationKind::$variant($data { .. })) => Ok($result::new(res)),
                    _ => Err(Error::InternalError(String::from(concat!(
                        "invalid conversion from UringOperationKind to ",
                        stringify!($result)
                    )))),
                }
            }
        }
    };
}

define_buf_io_result!(MadviseResult, Madvise, MadviseData);
define_buf_io_result!(ReadResult, Read, ReadData);
define_buf_io_result!(WriteResult, Write, WriteData);
define_empty_io_result!(FsyncResult, Fsync, FsyncData);
define_empty_io_result!(FdatasyncResult, Fdatasync, FsyncData);

use std::{fs::File, io::Write, os::unix::io::AsRawFd, ptr, time::Instant};

use aluring::{
    buf::UringBuf,
    result::IoResult,
    sqe::{Madvise, MadviseData, Sqe},
    Uring,
};

const FILE_SIZE: usize = 4 * 1024 * 1024;

#[test]
fn test_madvise() {
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(&vec![0xa; FILE_SIZE]).unwrap();
    f.flush().unwrap();

    let mut cached_reads = vec![];
    let mut uncached_reads = vec![];
    let ring = Uring::new(8).unwrap();
    unsafe {
        for _ in 0..20 {
            let f = File::open(f.path()).unwrap();
            let mut buf = vec![0u8; FILE_SIZE];

            let ptr = libc::mmap(
                ptr::null_mut(),
                FILE_SIZE,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                f.as_raw_fd(),
                0,
            );
            assert_ne!(ptr, libc::MAP_FAILED);
            let ptr = ptr as *mut u8;

            ptr::copy_nonoverlapping(ptr, buf.as_mut_ptr(), FILE_SIZE);
            let now = Instant::now();
            ptr::copy_nonoverlapping(ptr, buf.as_mut_ptr(), FILE_SIZE);
            cached_reads.push(now.elapsed().as_micros() as usize);

            ring.prepare_madvise(Sqe::new(MadviseData {
                advise: Madvise::DontNeed,
                buf: UringBuf::Raw {
                    ptr,
                    len: FILE_SIZE,
                },
            }))
            .unwrap()
            .wait()
            .unwrap();

            let now = Instant::now();
            ptr::copy_nonoverlapping(ptr, buf.as_mut_ptr(), FILE_SIZE);
            uncached_reads.push(now.elapsed().as_micros() as usize);

            libc::munmap(ptr as *mut _, FILE_SIZE);
        }
    }

    let average_cached_read: usize = cached_reads.iter().sum::<usize>() / cached_reads.len();
    let average_uncached_read: usize = uncached_reads.iter().sum::<usize>() / uncached_reads.len();
    assert!(average_cached_read < average_uncached_read);
    println!(
        "cached reads: {}, uncached reads: {}",
        average_cached_read, average_uncached_read
    );

    const ARENA_SIZE: usize = 1024 * 1024 * 1024 * 8;
    unsafe {
        let ptr = libc::mmap(
            ptr::null_mut(),
            ARENA_SIZE,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            0,
            0,
        );
        assert_ne!(ptr, libc::MAP_FAILED);
        let ptr = ptr as *mut u8;

        let buf = vec![0xau8; ARENA_SIZE];
        ptr::copy_nonoverlapping(buf.as_ptr(), ptr, ARENA_SIZE);
        assert_eq!(&buf, std::slice::from_raw_parts(ptr, ARENA_SIZE));
        ring.prepare_madvise(Sqe::new(MadviseData {
            advise: Madvise::DontNeed,
            buf: UringBuf::Raw {
                ptr,
                len: ARENA_SIZE,
            },
        }))
        .unwrap()
        .wait()
        .unwrap()
        .as_io_result()
        .unwrap();
        libc::madvise(ptr as *mut _, ARENA_SIZE, libc::MADV_DONTNEED);
        let slice = std::slice::from_raw_parts(ptr, ARENA_SIZE);
        assert_eq!(slice, &vec![0; ARENA_SIZE]);

        libc::munmap(ptr as *mut _, ARENA_SIZE);
    }
}

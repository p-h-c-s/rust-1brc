use std::ops::Deref;
use std::os::fd::AsRawFd;
use std::ptr::null_mut;
use std::{fs::File, os::raw::c_void};

use libc::{
    munmap,  mmap, size_t, MAP_FAILED, MAP_SHARED, PROT_READ,
};

/// Smart pointer type for a mmap. Handles munmap call.
pub struct Mmap<'a> {
    mmap_slice: &'a [u8],
}

/// To properly dispose of the mmap we have to manually call munmap.
/// So implementing drop for this smart-pointer type is necessary.
impl<'a> Drop for Mmap<'a> {
    fn drop(&mut self) {
        unsafe {
            munmap(
                self.mmap_slice.as_ptr() as *mut c_void,
                self.mmap_slice.len(),
            );
        }
    }
}

// anti-pattern for non-smart pointer types.
// ref: https://rust-unofficial.github.io/patterns/anti_patterns/deref.html
impl<'a> Deref for Mmap<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.mmap_slice
    }
}

impl<'a> Mmap<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { mmap_slice: data }
    }

    pub fn from_file(f: File) -> Self {
        let size = f.metadata().unwrap().len() as size_t;
        let prot = PROT_READ;
        let flags = MAP_SHARED;
        unsafe {
            let m = mmap(null_mut(), size, prot, flags, f.as_raw_fd(), 0);
            if m == MAP_FAILED {
                panic!("mmap failed");
            }
            // We can advise the kernel on how we intend to use the mmap.
            // But this did not improve my read performance
            // madvise(m, size, MADV_WILLNEED);
            return Self::new(std::slice::from_raw_parts(m as *const u8, size));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use std::path::Path;

    fn create_test_file(path: &Path, content: &[u8]) {
        let mut file = File::create(path).unwrap();
        file.write_all(content).unwrap();
    }

    fn remove_test_file(path: &Path) {
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
    }

    #[test]
    fn test_from_file() {
        let test_file_path = Path::new("test_file.txt");
        let test_content = b"Hello, mmap!";
        create_test_file(test_file_path, test_content);
        let file = File::open(test_file_path).unwrap();

        let my_struct = Mmap::from_file(file);

        assert_eq!(&*my_struct, test_content);
        remove_test_file(test_file_path);
    }
}

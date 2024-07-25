use std::os::fd::AsRawFd;
use std::ptr::{null_mut, NonNull};
use std::{fs::File, os::raw::c_void};

use libc::{mmap, size_t};
use libc::{munmap, PROT_READ, PROT_WRITE, MAP_SHARED, MAP_FAILED};

struct Mmap<'a> {
    mmap_slice: &'a mut [u8],
    mmap_addr: *mut NonNull<u8>,
    f_len: usize
}

impl<'a> Drop for Mmap<'a> {
    fn drop(&mut self) {
        unsafe {
            munmap(self.mmap_addr as *mut c_void, self.f_len);
        }
    }
}

impl<'a> Mmap<'a> {
    pub fn from_file(f: File) -> &'a [u8] {
        let size = f.metadata().unwrap().len() as size_t;
        let prot = PROT_READ;
        let flags =  MAP_SHARED;
        unsafe {
            let m = mmap(null_mut(), size, prot, flags, f.as_raw_fd(), 0);
            if m == MAP_FAILED {
                panic!("mmap failed");
            }
            return std::slice::from_raw_parts(m as *const u8, size)
        }
    }
}




#[cfg(test)]
mod tests {
    use std::fs::File;

    use super::Mmap;


    #[test]
    fn test_mmap() {
        let f = File::open("sample.txt").unwrap();
        
        let map = Mmap::from_file(f);
        let string = std::str::from_utf8(map).unwrap();
        let split = string.split_terminator("\n");
        let stdout = std::io::stdout().lock();
        for l in split {
            println!("{:?}", l);
        }
        // println!("{:?}", &map[1..100]);
    }
}
// re-export digest
pub use digest::{self, Digest, Update};

trait New {
    fn new() -> Self;
}

use std::io::{Read, Write};

//trait Update {
//    /// Process data, updating the internal state.
//    fn update(&mut self, data: impl AsRef<[u8]>);
//}

pub struct HashReader<R: Read, H> {
    read: R,
    hasher: H,
}

impl<R: Read, H> HashReader<R, H> {
    pub fn new(read: R, hasher: H) -> Self {
        HashReader { read, hasher }
    }
}

impl<R: Read, H: digest::OutputSizeUser> HashReader<R, H>
where
    H: Default + Digest,
{
    pub fn finalize(&mut self) -> digest::Output<H> {
        let mut swap = H::default();
        std::mem::swap(&mut self.hasher, &mut swap);
        swap.finalize()
    }
}

impl<R: Read, H: Update> Read for HashReader<R, H> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        let ret = self.read.read(buf);
        if let Ok(n) = ret {
            self.hasher.update(&buf[0..n]);
        }
        ret
    }
}

pub struct HashWriter<W: Write, H> {
    write: W,
    hasher: H,
}

impl<W: Write, H> HashWriter<W, H> {
    pub fn new(write: W, hasher: H) -> Self {
        HashWriter { write, hasher }
    }

    //    pub fn finalize(&mut self) -> digest::Output<H> {
    //        let mut swap = H::new();
    //        std::mem::swap(&mut self.hasher, &mut swap);
    //        swap.finalize()
    //    }
}

impl<W: Write, H: Update> Write for HashWriter<W, H> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, std::io::Error> {
        let ret = self.write.write(buf);
        if let Ok(n) = ret {
            self.hasher.update(&buf[0..n]);
        }
        ret
    }

    fn flush(&mut self) -> Result<(), std::io::Error> {
        self.write.flush()
    }
}

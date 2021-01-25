use zstd_seekable::{self, CStream, SeekableCStream};

pub struct SeekableCompress<'a, A: std::io::Write> {
    cstream: SeekableCStream,
    out: &'a mut A,
    // Tracks if we're done.
    wrote_seek_table: bool,
}

impl<'a, A: std::io::Write> SeekableCompress<'a, A> {
    pub fn new(
        out: &'a mut A,
        compression_level: usize,
        frame_size: usize,
    ) -> Result<Self, zstd_seekable::Error> {
        let cstream = SeekableCStream::new(compression_level, frame_size)?;

        Ok(SeekableCompress {
            cstream,
            out,
            wrote_seek_table: false,
        })
    }

    fn end_compression_internal(&mut self) -> std::io::Result<usize> {
        let mut buff_out = &mut *self.out_buffer();
        let mut wrote = 0;
        loop {
            let out_pos = self
                .cstream
                .end_stream(&mut buff_out)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            // No more data left in the stream, we're done.
            if out_pos == 0 {
                break;
            }
            wrote += self.out.write(&buff_out[..out_pos])?
        }
        Ok(wrote)
    }

    // Finish compressing the object. This writes out any remaining data in the
    // compressor and writes the final seek table to the end of the data. You
    // should always call this before the object is dropped: Drop will invoke
    // this function but will ignore any errors which is probably not something
    // you want to do: likely you'll end up with file that can't be decompressed
    // at the end or if you get very lucky, that can be fully decompressed but
    // not in a seeking fashion.
    //
    // Returns number of compressed bytes we ended up writing as part of the end
    // of the stream.
    pub fn end_compression(mut self) -> std::io::Result<usize> {
        // We don't have to check if we ended the stream already here: this
        // method can be invoked precisely once. The only other entry point to
        // end_compression_internal is the drop implementation and that will
        // perform its own check.
        let r = self.end_compression_internal();
        self.wrote_seek_table = true;
        r
    }

    // We make the output buffer on demand. The reasonining is that we don't
    // want to balloon the memory usage if we have many SeekableCompress
    // objects. The compression time should greatly outweigh the time it takes
    // to allocate the output buffer. If not, we can move a shared buffer into
    // the structure and just re-use it.
    fn out_buffer(&self) -> Box<[u8]> {
        vec![0; CStream::out_size()].into_boxed_slice()
    }
}

impl<'a, A: std::io::Write> Drop for SeekableCompress<'a, A> {
    fn drop(&mut self) {
        // The user may have already called end_compression in which case we
        // don't want to re-invoke the internal function.
        if !self.wrote_seek_table {
            // We shouldn't panic in drop() so we suppress any error. User is
            // supposed to call end_compression themselves.
            let _e = self.end_compression_internal();
        }
    }
}

impl<'a, A> std::io::Write for SeekableCompress<'a, A>
where
    A: std::io::Write,
{
    fn write(&mut self, mut buf: &[u8]) -> std::io::Result<usize> {
        let starting_len = buf.len();
        let mut buff_out = &mut *self.out_buffer();

        // We loop until whole input is consumed. However, perhaps we should
        // not: the documentation only says that it's OK to only consume part of
        // the input. Perhaps we should leave it to the user to re-supply rest
        // of input? I'm unsure what the normal behaviour is supposed to be.
        while buf.len() > 0 {
            let (out_pos, in_pos) = self
                .cstream
                .compress(&mut buff_out, buf)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            let compressed_data = &buff_out[..out_pos];
            // Maybe we didn't get any data to write yet and it lives in the
            // stream. Don't do any write in that case.
            if compressed_data.len() > 0 {
                self.out.write(compressed_data)?;
            }
            buf = &buf[in_pos..];
        }
        // We consumed the whole input. Note that we report back the length of
        // the original buffer rather than sum of self.out.write: we don't want
        // to report back the number of compressed bytes but rather how much of
        // the input buffer was consumed.
        Ok(starting_len)
    }

    // Calling flush does nothing to our stream but we do invoke the underlying
    // object flush.
    fn flush(&mut self) -> std::io::Result<()> {
        self.out.flush()
    }
}

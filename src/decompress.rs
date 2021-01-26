use std::{convert::TryFrom, fmt::Display, num::TryFromIntError};
use zstd_seekable::Seekable;

// The seek/read methods on this object will read/seek uncompress an underlying
// object and read/seek within it.
pub struct SeekableDecompress<'a, A> {
    seekable: Seekable<'a, A>,
    // We use this across read invocations to make sure we don't run off the end
    // of stream so just compute it once ahead of time.
    decompressed_size: u64,
    // Seek position in the decompressed data.
    decompressed_position: u64,
}

#[derive(Debug)]
pub enum Error {
    NoFrames,
    // Frame size was too big for u64.
    FrameTooLarge(TryFromIntError),
    // End of data was past u64.
    DataTooLarge,
    ZstdSeekable(zstd_seekable::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NoFrames => write!(
                f,
                "No frames found in the stream. Use regular decompression."
            ),
            Error::FrameTooLarge(e) => {
                write!(f, "Encountered a frame larger than we can work with: {}", e)
            }
            Error::DataTooLarge => write!(f, "Data larger than we can work with."),
            Error::ZstdSeekable(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for Error {}

impl<'a, A> SeekableDecompress<'a, A>
where
    A: std::io::Read + std::io::Seek,
{
    pub fn new(compressed: A) -> Result<Self, Error> {
        let seekable = Seekable::init(Box::new(compressed)).map_err(Error::ZstdSeekable)?;

        let decompressed_size = {
            let num_frames = seekable.get_num_frames();
            if num_frames == 0 {
                return Err(Error::NoFrames);
            }
            let last_frame_index = num_frames - 1;
            let last_frame_start = seekable.get_frame_decompressed_offset(last_frame_index);
            let last_frame_size = seekable.get_frame_decompressed_size(last_frame_index);
            match u64::try_from(last_frame_size) {
                Ok(last_frame_size) => match last_frame_start.checked_add(last_frame_size) {
                    None => return Err(Error::DataTooLarge),
                    Some(r) => r,
                },
                Err(e) => return Err(Error::FrameTooLarge(e)),
            }
        };

        Ok(SeekableDecompress {
            seekable,
            decompressed_size,
            decompressed_position: 0,
        })
    }
}

impl<'a, A> std::io::Seek for SeekableDecompress<'a, A> {
    // Seeking inside decompressed data does nothing except store the location
    // as there is no actual decompressed data on hand to seek in. We use this
    // location when we try to perform an actual read of the data.
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let (base_pos, offset) = match pos {
            std::io::SeekFrom::Start(pos) => {
                self.decompressed_position = pos;
                return Ok(pos);
            }
            std::io::SeekFrom::End(pos) => (self.decompressed_size, pos),
            std::io::SeekFrom::Current(pos) => (self.decompressed_position, pos),
        };
        let new_pos = if offset >= 0 {
            base_pos.checked_add(offset as u64)
        } else {
            base_pos.checked_sub((offset.wrapping_neg()) as u64)
        };
        match new_pos {
            Some(n) => {
                self.decompressed_position = n;
                Ok(self.decompressed_position)
            }
            None => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid seek to a negative or overflowing position",
            )),
        }
    }
}

impl<'a, A> std::io::Read for SeekableDecompress<'a, A> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let data_left = match self
            .decompressed_size
            .checked_sub(self.decompressed_position)
        {
            Some(data_left) if data_left > 0 => data_left,
            // We're at the end of data or past it. Just return straight away
            // with no bytes read.
            _ => return Ok(0),
        };

        // We know at this point we have some data remaining. The seekable
        // uncompressor requires that we pass a buffer that's at most the size
        // of the remaining data so even if the user passed in a large buffer,
        // make sure to only pass in the correctly-sized slice if there isn't
        // enough data to fill the whole buffer.
        let mut buf = match usize::try_from(data_left) {
            Ok(data_left) if data_left < buf.len() => &mut buf[..data_left],
            // The amount of data in data_left was too big for usize so we
            // can safely assume the existing buffer size _must_ be smaller
            // OR the buffer size was large enough to be filled with the
            // existing amount of data left at which point we can just use
            // the buffer as-is.
            _ => buf,
        };

        // We do one last final check, this is to check that the slice we now
        // want to write the data to actually is able to store any bytes at all:
        // there's no point passing asking to decompress 0 bytes of data.
        if buf.is_empty() {
            return Ok(0);
        }

        let our_error = |e| std::io::Error::new(std::io::ErrorKind::Other, e);
        let zstd_error = |e: zstd_seekable::Error| our_error(Error::ZstdSeekable(e));

        // We're finally done setting up the output buffer, actually read in the
        // decompressed data at current position now.
        let decompressed_bytes = self
            .seekable
            .decompress(&mut buf, self.decompressed_position)
            .map_err(zstd_error)?;

        // Bump the position by however many bytes we have managed to read in.
        {
            let decompressed_bytes = u64::try_from(decompressed_bytes)
                // Realistically we should have gotten DataTooLarge when creating
                // the stream. In general this should not happen unless you have 18
                // exabyte file or something like that.
                .map_err(|_e| our_error(Error::DataTooLarge))?;

            // We're past what we can store in u64. Just like above, realistically
            // won't happen.
            self.decompressed_position = self
                .decompressed_position
                .checked_add(decompressed_bytes)
                .ok_or_else(|| our_error(Error::DataTooLarge))?;
        }

        Ok(decompressed_bytes)
    }
}

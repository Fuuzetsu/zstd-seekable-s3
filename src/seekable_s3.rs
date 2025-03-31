use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::builders::GetObjectFluentBuilder;
use aws_sdk_s3::operation::get_object::GetObjectError;
use futures::TryFutureExt;
use std::convert::TryFrom;
use std::io::{Error, ErrorKind, Read, Seek};
use std::pin::Pin;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;

pub struct SeekableS3Object<'a> {
    req: GetObjectFluentBuilder,
    position: u64,
    // Updated when we first read the object.
    length: u64,
    body: Option<Pin<Box<dyn AsyncRead + Send>>>,
    runtime: &'a tokio::runtime::Runtime,
}

impl std::fmt::Debug for SeekableS3Object<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeekableS3Object")
            .field("req", &self.req)
            .field("position", &self.position)
            .field("length", &self.length)
            .finish()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum NewSeekableS3ObjectError {
    #[error("Content length not set in response.")]
    ContentLengthNotSet,
    #[error("Content length didn't fit into a u64, got {input}: {source}")]
    BadContentLength {
        input: i64,
        source: std::num::TryFromIntError,
    },
    #[error(transparent)]
    SdkError(#[from] SdkError<GetObjectError, aws_sdk_s3::config::http::HttpResponse>),
}

impl<'a> SeekableS3Object<'a> {
    pub fn new(
        runtime: &'a tokio::runtime::Runtime,
        req: GetObjectFluentBuilder,
    ) -> Result<Self, NewSeekableS3ObjectError> {
        // If for some reason we get request with range field filled, get rid of
        // it or we will end up with the wrong content length returned.
        // Alternatively we may want to use HeadObject request instead.
        let req = req.set_range(None);

        let object = runtime.block_on(req.clone().send())?;
        let body = Some(Box::pin(object.body.into_async_read()) as Pin<Box<dyn AsyncRead + Send>>);

        let Some(length) = object.content_length else {
            return Err(NewSeekableS3ObjectError::ContentLengthNotSet);
        };

        let length = match u64::try_from(length) {
            Ok(length) => length,
            Err(source) => {
                return Err(NewSeekableS3ObjectError::BadContentLength {
                    input: length,
                    source,
                })
            }
        };

        Ok(SeekableS3Object {
            req,
            position: 0,
            length,
            body,
            runtime,
        })
    }

    // Sets current position. If the position actually changes, invalidates the
    // current object body.
    //
    // You should only use this if you're not consuming the body. If the body is
    // being consumed, you just want to update the position directly based on
    // how much you've consumed.
    fn set_position(&mut self, new_position: u64) {
        if self.position != new_position {
            self.position = new_position;
            self.body = None;
        }
    }

    // Reads some data from the body while remebering to update the position.
    fn read_body(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if let Some(body) = &mut self.body {
            let bytes_read = self.runtime.block_on(body.read(buf))?;
            // If we managed to read something, make sure to update position.
            // This saves us work if we something calls seek into the new
            // position.
            self.position += bytes_read as u64;
            Ok(bytes_read)
        } else {
            // No body.
            Ok(0)
        }
    }
}

impl Read for SeekableS3Object<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // We're done reading, AWS API throws a fit for out-of-range range
        // requests so we exit early.
        if self.position >= self.length {
            return Ok(0);
        }

        // We may have a body already present in which case we just read from
        // it. Only if we don't have the body (for example, we performed a seek)
        // do we issue any new requests.
        if self.body.is_some() {
            return self.read_body(buf);
        }

        // We didn't have existing body to read from: probably we have done a
        // seek. Get the body at the new position, read some data and store the
        // new body for the future.

        let req = self
            .req
            .clone()
            .set_range(Some(format!("bytes={}-", self.position)));

        let get_object = req.send().map_err(|e| Error::new(ErrorKind::Other, e));

        let object = self.runtime.block_on(get_object)?;

        self.body = Some(Box::pin(object.body.into_async_read()) as Pin<Box<dyn AsyncRead + Send>>);

        self.read_body(buf)
    }
}

impl Seek for SeekableS3Object<'_> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        // Implementation roughly lifted from std::io::cursor Seek trait
        // implementation.
        let (base_pos, offset) = match pos {
            std::io::SeekFrom::Start(pos) => {
                self.set_position(pos);
                return Ok(pos);
            }
            std::io::SeekFrom::End(pos) => (self.length, pos),
            std::io::SeekFrom::Current(pos) => (self.position, pos),
        };
        let new_pos = if offset >= 0 {
            base_pos.checked_add(offset as u64)
        } else {
            base_pos.checked_sub((offset.wrapping_neg()) as u64)
        };
        match new_pos {
            Some(n) => {
                self.set_position(n);
                Ok(self.position)
            }
            None => Err(Error::new(
                ErrorKind::InvalidInput,
                "invalid seek to a negative or overflowing position",
            )),
        }
    }
}

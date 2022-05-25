use futures::TryFutureExt;
use rusoto_core::RusotoError;
use rusoto_s3::{GetObjectError, GetObjectRequest, S3Client, S3};
use std::convert::TryFrom;
use std::io::{Error, ErrorKind, Read, Seek};
use std::pin::Pin;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;

pub struct SeekableS3Object<'a, A> {
    client: A,
    req: GetObjectRequest,
    position: u64,
    // Updated when we first read the object.
    length: u64,
    body: Option<Pin<Box<dyn AsyncRead + Send>>>,
    runtime: &'a tokio::runtime::Runtime,
    // Limit reads to this amount of time.
    read_timeout: Option<std::time::Duration>,
}

impl<A: std::fmt::Debug> std::fmt::Debug for SeekableS3Object<'_, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeekableS3Object")
            .field("client", &self.client)
            .field("req", &self.req)
            .field("position", &self.position)
            .field("length", &self.length)
            .field("runtime", &self.runtime)
            .finish()
    }
}

impl<'a, A> SeekableS3Object<'a, A> {
    pub fn new(
        client: A,
        runtime: &'a tokio::runtime::Runtime,
        read_timeout: Option<std::time::Duration>,
        mut req: GetObjectRequest,
    ) -> Result<Result<Self, RusotoError<GetObjectError>>, tokio::time::error::Elapsed>
    where
        A: S3,
    {
        // If for some reason we get request with range field filled, get rid of
        // it or we will end up with the wrong content length returned.
        // Alternatively we may want to use HeadObject request instead.
        req.range = None;
        let get_object = client.get_object(req.to_owned());

        let object = match read_timeout {
            Some(timeout) => {
                let _executor = runtime.enter();
                runtime.block_on(tokio::time::timeout(timeout, get_object))?
            }
            None => runtime.block_on(get_object),
        };

        let object = match object {
            Ok(o) => o,
            Err(err) => return Ok(Err(err)),
        };

        let body = object
            .body
            // I don't understand why the cast is needed but otherwise we get
            //
            // note: expected enum `Option<Box<(dyn tokio::io::AsyncRead + Sync + std::marker::Send + 'static)>>`
            // found enum `Option<Box<impl std::marker::Send+Sync+tokio::io::AsyncRead>>`
            //
            // https://stackoverflow.com/questions/61259521/struct-with-boxed-impl-trait
            .map(|bs| Box::pin(bs.into_async_read()) as Pin<Box<dyn AsyncRead + Send>>);

        let length = match object.content_length {
            None => {
                return Ok(Err(RusotoError::Validation(
                    "Content length not set in response.".to_owned(),
                )))
            }
            Some(length) => match u64::try_from(length) {
                Ok(length) => length,
                Err(_e) => {
                    return Ok(Err(RusotoError::Validation(format!(
                        "Content length didn't fit into a u64, got {}",
                        length
                    ))))
                }
            },
        };

        Ok(Ok(SeekableS3Object {
            client,
            req,
            position: 0,
            length,
            body,
            runtime,
            read_timeout,
        }))
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
            let bytes_read = match self.read_timeout {
                Some(timeout) => {
                    let _executor = self.runtime.enter();
                    match self
                        .runtime
                        .block_on(tokio::time::timeout(timeout, body.read(buf)))
                    {
                        Ok(r) => r,
                        Err(timeout_err) => Err(Error::new(ErrorKind::TimedOut, timeout_err)),
                    }
                }

                None => self.runtime.block_on(body.read(buf)),
            }?;
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

    /// Set the read timeout to the given duration. Set to None to disable
    /// time-out.
    pub fn set_read_timeout(&mut self, read_timeout: Option<std::time::Duration>) {
        self.read_timeout = read_timeout;
    }
}

impl<'a, A> Read for SeekableS3Object<'_, A>
where
    A: S3,
{
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
        self.req.range = Some(format!("bytes={}-", self.position));

        let get_object = self
            .client
            .get_object(self.req.to_owned())
            .map_err(|e| Error::new(ErrorKind::Other, e));

        let object = match self.read_timeout {
            Some(timeout) => {
                let _executor = self.runtime.enter();
                match self
                    .runtime
                    .block_on(tokio::time::timeout(timeout, get_object))
                {
                    Ok(r) => r,
                    Err(timeout_err) => Err(Error::new(ErrorKind::TimedOut, timeout_err)),
                }
            }
            None => self.runtime.block_on(get_object),
        }?;

        self.body = object
            .body
            .map(|bs| Box::pin(bs.into_async_read()) as Pin<Box<dyn AsyncRead + Send>>);

        self.read_body(buf)
    }
}

impl<A> Seek for SeekableS3Object<'_, A> {
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

// Allows to simply say `s3.get_seekable_object` to be consistent with rest of
// rusoto API.
pub trait GetSeekableObject: Sized {
    fn get_seekable_object(
        self,
        runtime: &tokio::runtime::Runtime,
        read_timeout: Option<std::time::Duration>,
        input: GetObjectRequest,
    ) -> Result<
        Result<SeekableS3Object<'_, Self>, RusotoError<GetObjectError>>,
        tokio::time::error::Elapsed,
    >;
}

impl GetSeekableObject for S3Client {
    fn get_seekable_object(
        self,
        runtime: &tokio::runtime::Runtime,
        read_timeout: Option<std::time::Duration>,
        input: GetObjectRequest,
    ) -> Result<
        Result<SeekableS3Object<'_, Self>, RusotoError<GetObjectError>>,
        tokio::time::error::Elapsed,
    > {
        SeekableS3Object::new(self, runtime, read_timeout, input)
    }
}

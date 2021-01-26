use futures::executor::block_on;
use rusoto_core::RusotoError;
use rusoto_s3::{GetObjectError, GetObjectRequest, S3Client, S3};
use std::convert::TryFrom;
use std::io::{Error, ErrorKind, Read, Seek};
use tokio::io::AsyncReadExt;

#[derive(Debug)]
pub struct SeekableS3Object<'a, A> {
    client: &'a A,
    req: GetObjectRequest,
    position: u64,
    // Updated when we first read the object.
    length: u64,
}

impl<'a, A> SeekableS3Object<'a, A> {
    pub fn new(
        client: &'a A,
        mut req: GetObjectRequest,
    ) -> Result<Self, RusotoError<GetObjectError>>
    where
        A: S3,
    {
        // If for some reason we get request with range field filled, get rid of
        // it or we will end up with the wrong content length returned.
        // Alternatively we may want to use HeadObject request instead.
        req.range = None;
        let object = block_on(client.get_object(req.to_owned()))?;
        let length = match object.content_length {
            None => Err(RusotoError::Validation(
                "Content length not set in response.".to_owned(),
            )),
            Some(length) => match u64::try_from(length) {
                Ok(length) => Ok(length),
                Err(_e) => Err(RusotoError::Validation(format!(
                    "Content length didn't fit into a u64, got {}",
                    length
                ))),
            },
        }?;
        Ok(SeekableS3Object {
            client,
            req,
            position: 0,
            length,
        })
    }
}

impl<'a, A> Read for SeekableS3Object<'a, A>
where
    A: S3,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // We're done reading, AWS API throws a fit for out-of-range range
        // requests so we exit early.
        if self.position >= self.length {
            return Ok(0);
        }

        self.req.range = Some(format!("bytes={}-", self.position));
        let result = block_on(async {
            let object = self
                .client
                .get_object(self.req.to_owned())
                .await
                .map_err(|e| Error::new(ErrorKind::Other, e))?;
            // No async closures so we have to use match instead of map_or.
            match object.body {
                // I'm not sure if this is correct or empty body should be some
                // error... leave it for now.
                None => Ok(0),
                Some(body) => body.into_async_read().read(buf).await,
            }
        });
        // If we managed to read some bytes, remember how far we got so that
        // next time we read, we read a fresh chunk.
        if let Ok(bytes_read) = result {
            self.position += bytes_read as u64;
        }
        result
    }
}

impl<'a, A> Seek for SeekableS3Object<'a, A> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        // Implementation roughly lifted from std::io::cursor Seek trait
        // implementation.
        let (base_pos, offset) = match pos {
            std::io::SeekFrom::Start(pos) => {
                self.position = pos;
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
                self.position = n;
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
        &self,
        input: GetObjectRequest,
    ) -> Result<SeekableS3Object<'_, Self>, RusotoError<GetObjectError>>;
}

impl GetSeekableObject for S3Client {
    fn get_seekable_object(
        &self,
        input: GetObjectRequest,
    ) -> Result<SeekableS3Object<'_, Self>, RusotoError<GetObjectError>> {
        SeekableS3Object::new(self, input)
    }
}

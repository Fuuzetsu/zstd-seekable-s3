use std::{marker::PhantomData, pin::Pin};

use bytes::{BufMut, BytesMut};
use futures::{
    ready,
    stream::{FusedStream, Stream},
};
use pin_project_lite::pin_project;
use rusoto_core::ByteStream;
use rusoto_s3::UploadPartRequest;

// Uploads a stream of data.

pub trait StreamUploadParts {
    fn upload_parts<I, E>(
        self,
        part_template: UploadPartRequest,
        minimum_part_size: usize,
    ) -> UploadParts<Self, E>
    where
        Self: Stream<Item = Result<I, E>> + Sized,
        I: std::borrow::Borrow<[u8]>;
}

impl<S> StreamUploadParts for S {
    fn upload_parts<I, E>(
        self,
        part_template: UploadPartRequest,
        minimum_part_size: usize,
    ) -> UploadParts<Self, E>
    where
        S: Stream<Item = Result<I, E>>,
        I: std::borrow::Borrow<[u8]>,
    {
        UploadParts::new(self, part_template, minimum_part_size)
    }
}

// Chunk into parts for upload.
pin_project! {
    pub struct UploadParts<S, E> {
        #[pin]
        stream: S,
        input: BytesMut,
        next_part_number: i64,
        finished: bool,
        part_template: UploadPartRequest,
        minimum_part_size: usize,
        error_type: PhantomData<E>,
    }
}

impl<S, E> UploadParts<S, E> {
    fn new(stream: S, part_template: UploadPartRequest, minimum_part_size: usize) -> Self
    where
        S: Stream,
    {
        Self {
            stream,
            input: BytesMut::new(),
            next_part_number: 1,
            finished: false,
            part_template,
            minimum_part_size,
            error_type: PhantomData,
        }
    }

    fn next_input(
        self: &mut Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<S::Item>>
    where
        S: Stream,
    {
        self.as_mut().project().stream.poll_next(cx)
    }

    // Makes a part from internal buffer. Doesn't do any checks about whether we
    // have any data.
    //
    // Kind of inefficient if we read big chunks as we go through an extra buffer. Fix later.
    fn part_from_buffer(self: &mut Pin<&mut Self>) -> UploadPartRequest {
        let this = self.as_mut().project();
        let buffer: &mut BytesMut = this.input;
        let part_template: &UploadPartRequest = this.part_template;
        let req = UploadPartRequest {
            body: Some(ByteStream::from(Vec::from(&buffer[..]))),
            bucket: part_template.bucket.to_owned(),
            // As we're going through Vec in body, the size hint is set and
            // rusoto fills in content_length by itself.
            content_length: None,
            content_md5: part_template.content_md5.to_owned(),
            expected_bucket_owner: part_template.expected_bucket_owner.to_owned(),
            key: part_template.key.to_owned(),
            part_number: *this.next_part_number,
            request_payer: part_template.request_payer.to_owned(),
            sse_customer_algorithm: part_template.sse_customer_algorithm.to_owned(),
            sse_customer_key: part_template.sse_customer_key.to_owned(),
            sse_customer_key_md5: part_template.sse_customer_key_md5.to_owned(),
            upload_id: part_template.upload_id.to_owned(),
        };
        // Next part we make should have new number.
        *this.next_part_number += 1;
        // Clear the buffer, we copied the data we wanted now and future inputs
        // should fill it from the start.
        buffer.clear();
        req
    }

    fn accept_input(self: &mut Pin<&mut Self>, input: &[u8]) -> Option<UploadPartRequest> {
        let this = self.as_mut().project();
        let buffer: &mut BytesMut = this.input;
        buffer.put(input);
        // After combining whatever we had with new input, if we have enough for a part, yield one.
        if buffer.len() >= *this.minimum_part_size {
            Some(self.part_from_buffer())
        } else {
            None
        }
    }

    // Create last chunk if we have any data buffered and send completion
    // message.
    fn end_stream(self: &mut Pin<&mut Self>) -> Option<UploadPartRequest> {
        let this = self.as_mut().project();
        *this.finished = true;

        let buffer: &BytesMut = this.input;
        // Last chunk has no size restrictions.
        if !buffer.is_empty() {
            Some(self.part_from_buffer())
        } else {
            None
        }
    }

    fn finished(self: &mut Pin<&mut Self>) -> bool {
        *self.as_mut().project().finished
    }
}

impl<S, I, E> Stream for UploadParts<S, E>
where
    S: Stream<Item = Result<I, E>>,
    I: std::borrow::Borrow<[u8]>,
{
    type Item = Result<UploadPartRequest, E>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.finished() {
            return std::task::Poll::Ready(None);
        }

        std::task::Poll::Ready(loop {
            match ready!(self.next_input(cx)) {
                None => break self.end_stream().map(Ok),
                Some(Err(e)) => break Some(Err(e)),
                Some(Ok(bytes)) => {
                    // If we had enough input for a part, yield it. Otherwise we
                    // loop to accept more input.
                    if let Some(part) = self.accept_input(bytes.borrow()) {
                        break Some(Ok(part));
                    }
                }
            }
        })
    }
}

impl<S, I, E> FusedStream for UploadParts<S, E>
where
    S: Stream<Item = Result<I, E>>,
    I: std::borrow::Borrow<[u8]>,
{
    fn is_terminated(&self) -> bool {
        self.finished
    }
}

use bytes::Bytes;
use futures::{ready, stream::FusedStream, Stream};
use parking_lot::Mutex;
use pin_project_lite::pin_project;
use std::{convert::Infallible, marker::PhantomData, pin::Pin};
use zstd_seekable::{self, CStream, SeekableCStream};

pin_project! {
    pub struct Compress<S, E> {
        #[pin]
        stream: S,
        cstream: Mutex<SeekableCStream>,
        buf_out: Box<[u8]>,
        wrote_seek_table: bool,
        error_type: PhantomData<E>,
    }
}

impl<S, E> std::fmt::Debug for Compress<S, E>
where
    S: Stream + std::fmt::Debug,
    S::Item: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Compress")
            .field("stream", &self.stream)
            // .field("cstream", &self.cstream)
            .field("buf_out", &self.buf_out)
            .field("wrote_seek_table", &self.wrote_seek_table)
            .finish()
    }
}

pub trait StreamCompress {
    fn compress<I, E>(
        self,
        compression_level: usize,
        frame_size: usize,
    ) -> ZstdError<Compress<Self, E>>
    where
        Self: Stream<Item = Result<I, E>> + Sized,
        I: std::borrow::Borrow<[u8]>;
}

impl<S> StreamCompress for S {
    fn compress<I, E>(
        self,
        compression_level: usize,
        frame_size: usize,
    ) -> ZstdError<Compress<Self, E>>
    where
        // By having the bounds at the function level rather than trait level,
        // we get a better error message: when trying to use compress(), we'll
        // get a message pointing at the call stating which bounds are not met.
        // This is in contrast to putting the bounds on the impl itself: when we
        // do that, we get messages about the method not being found at all and
        // given a more cryptic error message about missing bounds on the S.
        Self: Stream<Item = Result<I, E>> + Sized,
        I: std::borrow::Borrow<[u8]>,
    {
        Compress::new(self, compression_level, frame_size)
    }
}

impl<S, E> Compress<S, E> {
    fn new<I>(stream: S, compression_level: usize, frame_size: usize) -> ZstdError<Self>
    where
        S: Stream<Item = Result<I, E>>,
        I: std::borrow::Borrow<[u8]>,
    {
        let cstream =
            parking_lot::const_mutex(SeekableCStream::new(compression_level, frame_size)?);
        let buf_out = vec![0; CStream::out_size()].into_boxed_slice();
        Ok(Self {
            stream,
            cstream,
            buf_out,
            wrote_seek_table: false,
            error_type: PhantomData,
        })
    }

    fn next_input<I>(
        self: &mut Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<S::Item>>
    where
        S: Stream<Item = Result<I, E>>,
        I: std::borrow::Borrow<[u8]>,
    {
        self.as_mut().project().stream.poll_next(cx)
    }

    fn compress_input(self: &mut Pin<&mut Self>, mut input: &[u8]) -> ZstdError<bytes::Bytes> {
        // Don't bother doing anything at all if we didn't get any input in.
        if input.is_empty() {
            return Ok(Bytes::new());
        }

        let this = self.as_mut().project();
        let cstream: &mut SeekableCStream = this.cstream.get_mut();
        let buf_out: &mut [u8] = this.buf_out;
        // It might seem wasteful to make a vector even if we end up only
        // decompressing once. However, Bytes::copy_from_slice just makes a
        // vector anyway and converts from there.
        let mut compressed_bytes = Vec::new();
        while !input.is_empty() {
            let (out_pos, in_pos) = cstream.compress(buf_out, input)?;
            compressed_bytes.extend_from_slice(&buf_out[..out_pos]);
            input = &input[in_pos..];
        }
        Ok(bytes::Bytes::from(compressed_bytes))
    }

    fn end_stream(self: &mut Pin<&mut Self>) -> ZstdError<Bytes> {
        let this = self.as_mut().project();
        let wrote_seek_table = this.wrote_seek_table;
        let cstream: &mut Mutex<SeekableCStream> = this.cstream;
        let buf_out: &mut [u8] = this.buf_out;

        let mut cstream = cstream.lock();
        let mut out_pos = cstream.end_stream(buf_out)?;
        let mut compressed_bytes = buf_out[..out_pos].to_vec();
        while out_pos > 0 {
            out_pos = cstream.end_stream(buf_out)?;
            compressed_bytes.extend_from_slice(&buf_out[..out_pos])
        }
        *wrote_seek_table = true;
        Ok(Bytes::from(compressed_bytes))
    }

    fn finished(self: &mut Pin<&mut Self>) -> bool {
        *self.as_mut().project().wrote_seek_table
    }
}

type ZstdError<A> = std::result::Result<A, zstd_seekable::Error>;

#[derive(Debug)]
pub enum CompressError<E> {
    ZstdError(zstd_seekable::Error),
    Underlying(E),
}

impl From<CompressError<Infallible>> for zstd_seekable::Error {
    fn from(e: CompressError<Infallible>) -> Self {
        match e {
            CompressError::ZstdError(e) => e,
            CompressError::Underlying(inf) => panic!("The impossible happened: {}", inf),
        }
    }
}

impl<E: std::fmt::Display> std::fmt::Display for CompressError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressError::ZstdError(e) => write!(f, "Compression error: {}", e),
            CompressError::Underlying(e) => write!(f, "Underlying error: {}", e),
        }
    }
}

impl<E: std::error::Error + std::fmt::Display + 'static> std::error::Error for CompressError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CompressError::ZstdError(_) => None,
            CompressError::Underlying(e) => Some(e),
        }
    }
}

impl<S, I, E> Stream for Compress<S, E>
where
    S: Stream<Item = Result<I, E>>,
    I: std::borrow::Borrow<[u8]>,
{
    type Item = std::result::Result<Bytes, CompressError<E>>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // We've already consumed everything and finalised our compression
        // stream. Yield nothing. Notably, we don't want to poke the upstream
        // again.
        if self.finished() {
            return std::task::Poll::Ready(None);
        }

        std::task::Poll::Ready(loop {
            match ready!(self.next_input(cx)) {
                None => match self.end_stream() {
                    Err(e) => break Some(Err(CompressError::ZstdError(e))),
                    Ok(compressed_data) => {
                        if compressed_data.is_empty() {
                            break None;
                        } else {
                            break Some(Ok(compressed_data));
                        }
                    }
                },
                Some(Err(e)) => break Some(Err(CompressError::Underlying(e))),
                Some(Ok(bytes)) => match self.compress_input(bytes.borrow()) {
                    Err(e) => break Some(Err(CompressError::ZstdError(e))),
                    Ok(compressed_data) => {
                        // Maybe we want to return 0 length Bytes unconditionally?
                        // Who knows.
                        if !compressed_data.is_empty() {
                            break Some(Ok(compressed_data));
                        }
                    }
                },
            }
        })
    }
}

impl<S, I, E> FusedStream for Compress<S, E>
where
    S: Stream<Item = Result<I, E>>,
    I: std::borrow::Borrow<[u8]>,
{
    fn is_terminated(&self) -> bool {
        self.wrote_seek_table
    }
}

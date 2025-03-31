use bpaf::{Bpaf, Parser};
use futures::{StreamExt, TryStreamExt};
use parking_lot::Mutex;
use std::sync::Arc;
use std::{
    cmp::Ordering,
    io::{Read, Seek},
};
use zstd_seekable_s3::{SeekableDecompress, SeekableS3Object, StreamCompress};

#[derive(Bpaf)]
/// Compress and uncompress some S3 object via seekable interfaces.
struct Opt {
    #[bpaf(long, fallback(256), display_fallback)]
    buffer_size: usize,
    #[bpaf(long, fallback(5000), display_fallback)]
    num_lines: u64,
    #[bpaf(long, fallback(1024), display_fallback)]
    frame_size: usize,
    #[bpaf(long, fallback(1), display_fallback)]
    compression_level: usize,
    #[bpaf(long)]
    /// Bucket to upload object to.
    bucket: String,
    #[bpaf(long)]
    /// Object key to upload at.
    key: String,
}

// We compress a bunch of lines into a given location in S3. We track where the
// uncompressed half of these starts. Then we show that we can uncompress the
// data from the middle.
//
// Beware: the resulting object is not deleted afterwards. You have to do it
// yourself.
fn main() {
    let Opt {
        buffer_size,
        num_lines,
        frame_size,
        compression_level,
        bucket,
        key,
    } = opt().to_options().run();

    env_logger::init();

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let uncompressed_bytes_until_middle: Arc<Mutex<Option<usize>>> = Arc::new(Mutex::new(None));
    let s3 = runtime.block_on(async {
        // Generate some line stream, track how many uncompressed bytes we're
        // yielding until we get to half-way point.
        struct Lines {
            current: u64,
            max_lines: u64,
            uncompressed_bytes_until_middle: usize,
            out_ref: Arc<Mutex<Option<usize>>>,
        }

        let uncompressed_lines_stream = futures::stream::unfold(
            Lines {
                current: 0,
                max_lines: num_lines,
                uncompressed_bytes_until_middle: 0,
                out_ref: uncompressed_bytes_until_middle.clone(),
            },
            |mut lines: Lines| async {
                if lines.current >= lines.max_lines {
                    None
                } else {
                    let line = format!("This is line {}.\n", lines.current);
                    let middle = lines.max_lines / 2;

                    match lines.current.cmp(&middle) {
                        Ordering::Less => {
                            lines.uncompressed_bytes_until_middle += line.len();
                        }
                        Ordering::Equal => {
                            // If we're right in middle, write out the count into the
                            // shared reference. We don't touch the reference otherwise.
                            let mut x = lines.out_ref.lock();
                            *x = Some(lines.uncompressed_bytes_until_middle);
                        }
                        Ordering::Greater => {}
                    }
                    lines.current += 1;
                    Some((bytes::Bytes::copy_from_slice(line.as_bytes()), lines))
                }
            },
        );

        let compressed_lines_stream = uncompressed_lines_stream
            .map(infallible_err)
            .compress(compression_level, frame_size)
            .unwrap();
        tokio::pin!(compressed_lines_stream);

        let mut body = vec![];
        // Just store in memory for the purpose of example. User can put it in a
        // chunked stream or whatever and do multi-part if it's large, the sky's
        // the limit.
        while let Some(bs) = compressed_lines_stream.try_next().await.unwrap() {
            body.extend(bs)
        }

        let s3 = aws_sdk_s3::Client::new(
            &aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await,
        );

        s3.put_object()
            .bucket(bucket.clone())
            .key(key.clone())
            .body(aws_sdk_s3::primitives::ByteStream::from(body))
            .send()
            .await
            .unwrap();

        s3
    });

    // We're done uploading the object so we should have the information about
    // the uncompressed part in the shared reference.
    let uncompressed_bytes_until_middle = (*uncompressed_bytes_until_middle.lock()).unwrap();

    // Now that the object should be on S3, we will ask for some small amount of
    // uncompressed data from the middle: the program knows how to to translate
    // this into compressed data range and ask for it on S3, without downloading
    // the whole file.
    {
        let req = s3.get_object().bucket(bucket).key(key);

        // We get a wrapper over S3 object that does knows how to do seeking of a file. Note however that this is just the raw data!
        let seekable_raw_object = SeekableS3Object::new(&runtime, req).unwrap();

        // We wrap the seekable S3 object with a shim that actually knows about the
        // compression.
        let mut seekable_uncompressed_object =
            SeekableDecompress::new(seekable_raw_object).unwrap();

        // We can now read/seek in the seekable object as if it was just
        // uncompressed data and any decompression and offset translation will
        // happen on the fly. Let's seek to the middle of the lines.
        seekable_uncompressed_object
            .seek(std::io::SeekFrom::Start(
                uncompressed_bytes_until_middle as u64,
            ))
            .unwrap();
        // Make a small buffer for response, it should be enough to get the point
        // across. The size of this buffer determines how much we read in. Normal
        // read behaviour is implemented: invoking read() gets subsequent version of
        // the file: you could read() an S3 file all the way from the start to the
        // end if you wanted which would basically result in a normal decompression.
        let mut decompressed_content = vec![0; buffer_size];
        let decompressed_bytes = seekable_uncompressed_object
            .read(decompressed_content.as_mut_slice())
            .unwrap();
        let string_data = std::str::from_utf8(&decompressed_content[..decompressed_bytes]).unwrap();
        println!(
            "S3 content from decompressed bytes {} to {}",
            uncompressed_bytes_until_middle,
            uncompressed_bytes_until_middle + decompressed_bytes
        );
        println!("{}", string_data);
    }
}

fn infallible_err<T>(t: T) -> Result<T, std::convert::Infallible> {
    Ok(t)
}

use futures::StreamExt;
use std::{
    cmp::Ordering,
    io::{Read, Seek},
};
use structopt::StructOpt;
use tempfile::tempfile;
use tokio::io::AsyncWriteExt;
use zstd_seekable_s3::{SeekableDecompress, StreamCompress};

#[derive(StructOpt)]
#[structopt(
    name = "roundtrip_file",
    about = "Compress and uncompress some file via seekable interfaces.",
    rename_all = "kebab"
)]
struct Opt {
    #[structopt(default_value = "256")]
    buffer_size: usize,
    #[structopt(default_value = "5000")]
    num_lines: u64,
    #[structopt(default_value = "1024")]
    frame_size: usize,
    #[structopt(default_value = "1")]
    compression_level: usize,
}

// We compress a bunch of lines into a given location. We track where the
// uncompressed half of these starts. Then we show that we can uncompress the
// data from the middle.
#[tokio::main]
async fn main() {
    let opt = Opt::from_args();

    // Make some file on the filesystem we can compress to.
    let mut tempfile = tokio::fs::File::from(tempfile().unwrap());
    // We want to start decompression from roughly the middle line so we track
    // how many bytes that is. Note that we track _uncompressed_ bytes which is
    // the thing we want to seek by later.
    let uncompressed_bytes_until_middle =
        std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    // Create stream of compressed data, storing middle of uncompressed data. We
    // write the stream out to a file.
    {
        struct Lines {
            current: usize,
            uncompressed_bytes_until_middle: usize,
        }
        let middle = (opt.num_lines / 2) as usize;
        let max_lines = opt.num_lines as usize;
        fn infallible_err<T>(t: T) -> Result<T, std::convert::Infallible> {
            Ok(t)
        }
        let compressed_lines = futures::stream::unfold(
            Lines {
                current: 0,
                uncompressed_bytes_until_middle: 0,
            },
            |mut lines: Lines| async {
                if lines.current >= max_lines {
                    None
                } else {
                    let line = format!("This is line {}.\n", lines.current);
                    match lines.current.cmp(&middle) {
                        Ordering::Less => lines.uncompressed_bytes_until_middle += line.len(),
                        Ordering::Equal => {
                            uncompressed_bytes_until_middle.store(
                                lines.uncompressed_bytes_until_middle,
                                std::sync::atomic::Ordering::SeqCst,
                            );
                        }
                        Ordering::Greater => {}
                    }
                    lines.current += 1;
                    Some((bytes::Bytes::copy_from_slice(line.as_bytes()), lines))
                }
            },
        )
        .map(infallible_err)
        .compress(opt.compression_level, opt.frame_size)
        .unwrap();

        let mut compressed_lines = Box::pin(compressed_lines);
        // Write all the content out to the file.
        while let Some(ebytes) = compressed_lines.next().await {
            match ebytes {
                Ok(bytes) => tempfile.write_all(&bytes).await.unwrap(),
                Err(e) => panic!("{}", zstd_seekable::Error::from(e)),
            }
        }
    }
    // We should be finished writing everything by now and have the decompressed
    // position of the middle line.
    let uncompressed_bytes_until_middle =
        uncompressed_bytes_until_middle.load(std::sync::atomic::Ordering::SeqCst);

    // We now go back into the std::fs::File and use the usual ecosystem to read
    // the file. First thing we do is wrap the handle in a SeekableDecompressed
    // object: this is what automatically translates any seeks and reads using
    // decompressed byte values into seeks of compressed content and automatic
    // decompression.

    let mut tempfile = SeekableDecompress::new(tempfile.into_std().await).unwrap();
    tempfile
        .seek(std::io::SeekFrom::Start(
            uncompressed_bytes_until_middle as u64,
        ))
        .unwrap();

    // Read some data, put it back into UTF8 and show it on the screen.
    {
        let mut decompressed_content = vec![0; opt.buffer_size];
        let decompressed_bytes = tempfile.read(decompressed_content.as_mut_slice()).unwrap();
        let string_data = std::str::from_utf8(&decompressed_content[..decompressed_bytes]).unwrap();
        // From here you should see lines from the middle of the file, without
        // relation to where the compressed data might actually be in the file.
        println!(
            "Content from decompressed bytes {} to {}",
            uncompressed_bytes_until_middle,
            uncompressed_bytes_until_middle + decompressed_bytes
        );
        println!("{}", string_data);
    }
}

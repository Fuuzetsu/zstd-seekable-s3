use bpaf::{Bpaf, Parser};
use std::io::{Read, Write};
use std::path::PathBuf;
use zstd_seekable_s3::{SeekableDecompress, SeekableS3Object};

#[derive(Debug, Bpaf)]

/// Decompress some seekable zstd S3 object to a file.
struct Opt {
    #[bpaf(long)]
    /// Bucket that holds the object.
    bucket: String,
    #[bpaf(long)]
    /// Object key to upload at.
    key: String,
    #[bpaf(long)]
    /// File to write decompressed output to.
    output_file: PathBuf,
}

// We compress a bunch of lines into a given location in S3. We track where the
// uncompressed half of these starts. Then we show that we can uncompress the
// data from the middle.
//
// Beware: the resulting object is not deleted afterwards. You have to do it
// yourself.
fn main() {
    let Opt {
        bucket,
        key,
        output_file,
    } = opt().to_options().run();

    env_logger::init();

    // Now that the object should be on S3, we will ask for some small amount of
    // uncompressed data from the middle: the program knows how to to translate
    // this into compressed data range and ask for it on S3, without downloading
    // the whole file.
    {
        // let req = GetObjectRequest {
        //     bucket: opt.bucket.to_owned(),
        //     key: opt.key.to_owned(),
        //     ..Default::default()
        // };
        let rt = tokio::runtime::Runtime::new().unwrap();

        let s3 = rt.block_on(async {
            aws_sdk_s3::Client::new(
                &aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await,
            )
        });
        let req = s3.get_object().bucket(bucket).key(key);
        // We get a wrapper over S3 object that does knows how to do seeking of a file. Note however that this is just the raw data!
        let seekable_raw_object = SeekableS3Object::new(&rt, req).unwrap();

        // We wrap the seekable S3 object with a shim that actually knows about the
        // compression.
        let mut seekable_uncompressed_object =
            SeekableDecompress::new(seekable_raw_object).unwrap();

        // Create the file to output to.
        let mut output = std::fs::File::create(output_file).unwrap();

        let mut output_buffer = [0; 1024 * 512];
        loop {
            let n = seekable_uncompressed_object
                .read(&mut output_buffer)
                .unwrap();
            if n == 0 {
                output.flush().unwrap();
                break;
            }
            output.write_all(&output_buffer[..n]).unwrap();
        }
    }
}

use env_logger;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::DefaultCredentialsProvider;
use rusoto_s3::{GetObjectRequest, PutObjectRequest, S3Client, S3};
use rusoto_sts::{StsAssumeRoleSessionCredentialsProvider, StsClient};
use std::io::{Read, Seek, Write};
use structopt::StructOpt;
use zstd_seekable_s3::{GetSeekableObject, SeekableCompress, SeekableDecompress};

#[derive(Debug, StructOpt)]
#[structopt(
    name = "roundtrip_s3",
    about = "Compress and uncompress some S3 object via seekable interfaces.",
    rename_all = "kebab"
)]
struct Opt {
    #[structopt(long, default_value = "256")]
    buffer_size: usize,
    #[structopt(long, default_value = "5000")]
    num_lines: u64,
    #[structopt(long, default_value = "1024")]
    frame_size: usize,
    #[structopt(long, default_value = "1")]
    compression_level: usize,
    #[structopt(long, help = "Bucket to upload object to.")]
    bucket: String,
    #[structopt(long, help = "Object key to upload at.")]
    key: String,
    #[structopt(long, help = "Region the bucket is in.")]
    region: Region,
    #[structopt(long, help = "Role to assume, if any.")]
    role_arn: Option<String>,
}

// We compress a bunch of lines into a given location in S3. We track where the
// uncompressed half of these starts. Then we show that we can uncompress the
// data from the middle.
//
// Beware: the resulting object is not deleted afterwards. You have to do it
// yourself.
#[tokio::main]
async fn main() {
    let opt = Opt::from_args();

    env_logger::init();

    let sts = StsClient::new(opt.region.to_owned());

    let http_client = HttpClient::new().unwrap();
    // Make the S3 client. If the user specified a role, make sure to assume it
    // and refresh it as needed. Otherwise, just use the default credentials
    // provider which refreshes credentials as-needed, if-needed.
    let s3: S3Client = match opt.role_arn {
        Some(role_arn) => {
            let provider = StsAssumeRoleSessionCredentialsProvider::new(
                sts,
                role_arn,
                "zstd-seekable-s3-example-roundtrip-s3".to_string(),
                None,
                None,
                None,
                None,
            );
            S3Client::new_with(
                http_client,
                rusoto_credential::AutoRefreshingProvider::new(provider).unwrap(),
                opt.region.to_owned(),
            )
        }
        None => {
            let provider = DefaultCredentialsProvider::new().unwrap();
            S3Client::new_with(http_client, provider, opt.region.to_owned())
        }
    };

    // Start by compressing some data and sending it to S3.
    let mut uncompressed_bytes_until_middle = 0;
    {
        // We can construct StreamingBody however we want such as from a file,
        // generating it etc. We'll just generate a bunch of content on the fly. For
        // the sake of demonstration, we simply store the whole compressed data in
        // memory.
        let mut compressed_data = Vec::new();
        let mut compress =
            SeekableCompress::new(&mut compressed_data, opt.compression_level, opt.frame_size)
                .unwrap();
        // Compress each line in memory.
        for line_ix in 0..opt.num_lines {
            let line = format!("This is line {}.\n", line_ix);
            let uncompressed_data: &[u8] = line.as_ref();
            if (line_ix as u64) < (opt.num_lines / 2) {
                uncompressed_bytes_until_middle += uncompressed_data.len();
            }
            compress.write(line.as_ref()).unwrap();
        }
        // Make sure that once we're done writing data, we call end_compression
        // which will write the seek table.
        compress.end_compression().unwrap();

        let req = PutObjectRequest {
            bucket: opt.bucket.to_owned(),
            key: opt.key.to_owned(),
            body: Some(compressed_data.into()),
            ..Default::default()
        };
        // Upload the data.
        s3.put_object(req).await.unwrap();
    }

    // Now that the object should be on S3, we will ask for some small amount of
    // uncompressed data from the middle: the program knows how to to translate
    // this into compressed data range and ask for it on S3, without downloading
    // the whole file.
    {
        let req = GetObjectRequest {
            bucket: opt.bucket.to_owned(),
            key: opt.key.to_owned(),
            ..Default::default()
        };
        // We get a wrapper over S3 object that does knows how to do seeking of a file. Note however that this is just the raw data!
        let seekable_raw_object = s3.get_seekable_object(req).unwrap();
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
        let mut decompressed_content = [0; 256];
        let decompressed_bytes = seekable_uncompressed_object
            .read(&mut decompressed_content)
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

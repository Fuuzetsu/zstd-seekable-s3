use rusoto_core::{HttpClient, Region};
use rusoto_credential::DefaultCredentialsProvider;
use rusoto_s3::{GetObjectRequest, S3Client};
use rusoto_sts::{StsAssumeRoleSessionCredentialsProvider, StsClient};
use std::io::{Read, Write};
use std::path::PathBuf;
use structopt::StructOpt;
use zstd_seekable_s3::{GetSeekableObject, SeekableDecompress};

#[derive(Debug, StructOpt)]
#[structopt(
    name = "decompress_s3",
    about = "Decompress some seekable zstd S3 object to a file.",
    rename_all = "kebab"
)]
struct Opt {
    #[structopt(long, help = "Bucket that holds the object.")]
    bucket: String,
    #[structopt(long, help = "Object key to upload at.")]
    key: String,
    #[structopt(long, help = "Region the bucket is in.")]
    region: Region,
    #[structopt(long, help = "Role to assume, if any.")]
    role_arn: Option<String>,
    #[structopt(long, help = "File to write decompressed output to.")]
    output_file: PathBuf,
}

// We compress a bunch of lines into a given location in S3. We track where the
// uncompressed half of these starts. Then we show that we can uncompress the
// data from the middle.
//
// Beware: the resulting object is not deleted afterwards. You have to do it
// yourself.
fn main() {
    let opt = Opt::from_args();

    env_logger::init();

    let sts = StsClient::new(opt.region.to_owned());

    let http_config = {
        let mut config = rusoto_core::request::HttpConfig::new();
        // https://aws.amazon.com/premiumsupport/knowledge-center/s3-socket-connection-timeout-error/
        config.pool_idle_timeout(core::time::Duration::from_secs(20));
        config
    };
    let http_client = HttpClient::new_with_config(http_config).unwrap();
    // Make the S3 client. If the user specified a role, make sure to assume it
    // and refresh it as needed. Otherwise, just use the default credentials
    // provider which refreshes credentials as-needed, if-needed.
    let s3: S3Client = match opt.role_arn {
        Some(role_arn) => {
            let provider = StsAssumeRoleSessionCredentialsProvider::new(
                sts,
                role_arn,
                "zstd-seekable-s3-example-decompress-s3".to_string(),
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
        let rt = tokio::runtime::Runtime::new().unwrap();

        // We get a wrapper over S3 object that does knows how to do seeking of a file. Note however that this is just the raw data!
        let seekable_raw_object = s3.get_seekable_object(&rt, None, req).unwrap().unwrap();

        // We wrap the seekable S3 object with a shim that actually knows about the
        // compression.
        let mut seekable_uncompressed_object =
            SeekableDecompress::new(seekable_raw_object).unwrap();

        // Create the file to output to.
        let mut output = std::fs::File::create(opt.output_file).unwrap();
        //let mut output = std::io::BufWriter::new(output);

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

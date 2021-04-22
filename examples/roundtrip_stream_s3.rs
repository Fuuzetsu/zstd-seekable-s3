use futures::{StreamExt, TryStreamExt};
use parking_lot::Mutex;
use rusoto_core::{HttpClient, Region, RusotoError};
use rusoto_credential::DefaultCredentialsProvider;
use rusoto_s3::{
    AbortMultipartUploadRequest, CompleteMultipartUploadRequest, CompletedMultipartUpload,
    CompletedPart, CreateMultipartUploadRequest, GetObjectRequest, S3Client, UploadPartError,
    UploadPartRequest, S3,
};
use rusoto_sts::{StsAssumeRoleSessionCredentialsProvider, StsClient};
use std::sync::Arc;
use std::{
    cmp::Ordering,
    io::{Read, Seek},
};
use structopt::StructOpt;
use zstd_seekable_s3::{GetSeekableObject, SeekableDecompress, StreamCompress, StreamUploadParts};

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
    let s3: S3Client = match opt.role_arn.to_owned() {
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

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let uncompressed_bytes_until_middle: Arc<Mutex<Option<usize>>> = Arc::new(Mutex::new(None));
    runtime.block_on(async {
        // Generate some line stream, track how many uncompressed bytes we're
        // yielding until we get to half-way point.
        struct Lines {
            current: u64,
            max_lines: u64,
            uncompressed_bytes_until_middle: usize,
            out_ref: Arc<Mutex<Option<usize>>>,
        };

        let uncompressed_lines_stream = futures::stream::unfold(
            Lines {
                current: 0,
                max_lines: opt.num_lines,
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
            .compress(1, 1024)
            .unwrap();

        // We don't know the size of the output so we must either dump the
        // content to memory or use multi-part uploads. We go with the latter
        // for the example.
        let req = CreateMultipartUploadRequest {
            bucket: opt.bucket.to_owned(),
            key: opt.key.to_owned(),
            ..Default::default()
        };

        let upload_id = s3
            .create_multipart_upload(req)
            .await
            .unwrap()
            .upload_id
            .unwrap();

        let part_template = UploadPartRequest {
            bucket: opt.bucket.to_owned(),
            key: opt.key.to_owned(),
            upload_id: upload_id.to_owned(),
            ..Default::default()
        };

        // Compressed_lines_stream may fail when compressing and we may also
        // fail while uploading parts. We want to handle both failures,
        // whichever stream they come from. This will allow us to cancel the
        // multi-part upload if something went wrong.
        #[derive(Debug)]
        enum Error {
            CompressionError(zstd_seekable::Error),
            PartUploadError(RusotoError<UploadPartError>),
        };

        // A collection of uploaded parts or a failure.
        let completed_parts = compressed_lines_stream
            // Lift the compression error into common type
            .map_err(|e| Error::CompressionError(e.into()))
            // Pass to stream that chunks into parts (TODO: this is just framing
            // data at this point, maybe we can use some tokio helpers?)
            .upload_parts(part_template, 5 * 1024 * 1024)
            // Actually perform upload for each part.
            .and_then(|part| async {
                let part_number = part.part_number;
                s3.upload_part(part)
                    .await
                    .map(|out| CompletedPart {
                        e_tag: out.e_tag,
                        part_number: Some(part_number),
                    })
                    .map_err(Error::PartUploadError)
            })
            // We have to submit the information we get after each upload later
            // on to complete the upload so we collect it.
            .try_collect()
            .await;

        let abort_req = AbortMultipartUploadRequest {
            bucket: opt.bucket.to_owned(),
            key: opt.key.to_owned(),
            upload_id: upload_id.to_owned(),
            ..Default::default()
        };
        let completed_parts = match completed_parts {
            // We have failed to upload all parts cleanly, possibly something
            // failed during compression. Cancel the upload.
            Err(e) => {
                match s3.abort_multipart_upload(abort_req.to_owned()).await {
                    // Now that the abort went OK, panic with whatever thing
                    // went wrong earlier.
                    Ok(_) => panic!("{:?}", e),
                    // Even the multipart upload failed, panic with both errors.
                    Err(abort_e) => panic!("parts: {:?}, abort: {}", e, abort_e),
                }
            }
            Ok(completed_parts) => completed_parts,
        };

        // Now that all the parts were uploaded successfully, complete the
        // upload.
        let req = CompleteMultipartUploadRequest {
            bucket: opt.bucket.to_owned(),
            key: opt.key.to_owned(),
            upload_id: upload_id.to_owned(),
            multipart_upload: Some(CompletedMultipartUpload {
                parts: Some(completed_parts),
            }),
            ..Default::default()
        };
        // Abort if completion goes wrong.
        if let Err(e) = s3.complete_multipart_upload(req).await {
            match s3.abort_multipart_upload(abort_req).await {
                Err(abort_e) => panic!("complete: {}, abort: {}", e, abort_e),
                Ok(_) => panic!("{}", e),
            }
        };
    });

    // We're done uploading the object so we should have the information about
    // the uncompressed part in the shared reference.
    let uncompressed_bytes_until_middle = (*uncompressed_bytes_until_middle.lock()).unwrap();

    // Now that the object should be on S3, we will ask for some small amount of
    // uncompressed data from the middle: the program knows how to to translate
    // this into compressed data range and ask for it on S3, without downloading
    // the whole file.
    {
        let req = GetObjectRequest {
            bucket: opt.bucket.to_owned(),
            key: opt.key,
            ..Default::default()
        };
        // We get a wrapper over S3 object that does knows how to do seeking of a file. Note however that this is just the raw data!
        let seekable_raw_object = s3.get_seekable_object(&runtime, req).unwrap();
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

fn infallible_err<T>(t: T) -> Result<T, std::convert::Infallible> {
    Ok(t)
}

use std::io::{Read, Seek, Write};
use structopt::StructOpt;
use tempfile::tempfile;
use zstd_seekable_s3::{SeekableCompress, SeekableDecompress};

#[derive(Debug, StructOpt)]
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
    let mut tempfile = tempfile().unwrap();
    // We want to start decompression from roughly the middle line so we track
    // how many bytes that is. Note that we track _uncompressed_ bytes which is
    // the thing we want to seek by later.
    let mut uncompressed_bytes_until_middle = 0;
    {
        // Wrap the reference: anything we write here will be automatically
        // compressed by zstd into a seekable format.
        let mut tempfile =
            SeekableCompress::new(&mut tempfile, opt.compression_level, opt.frame_size).unwrap();

        for line_ix in 0..opt.num_lines {
            let line = format!("This is line {}.\n", line_ix);
            // Note we don't track how many compressed bytes we wrote: we only
            // want to talk about decompressed values as those are the
            // interesting ones.
            tempfile.write_all(line.as_ref()).unwrap();
            if line_ix < (opt.num_lines / 2) {
                uncompressed_bytes_until_middle += line.len();
            }
        }
        // We have to invoke end_compression to write a seek table at the end. It
        // will also happen automagically at drop time but that will ignore errors:
        // you probably do NOT want that.
        tempfile.end_compression().unwrap();
    }

    // We're done compressing now. We go to the start of the compressed file and
    // set it read-only just to show we're no longer tampering with it. In real
    // code, you of course wouldn't be reading the file you just spent time
    // compressing and you'd just open an old handle.
    {
        tempfile.seek(std::io::SeekFrom::Start(0)).unwrap();
        let mut permissions = tempfile.metadata().unwrap().permissions();
        permissions.set_readonly(true);
    }

    // Now that we are at the start of our temporary file (just as we would be
    // if we just opened it), we want to decompress it from the middle. and show
    // some of the content. We tracked earlier where the middle (of the lines)
    // was.
    let mut tempfile = SeekableDecompress::new(tempfile).unwrap();
    // Now we have a SeekableDecompressed: any reads at given byte locations
    // will result in reads in the _decompressed_ data which is exactly what we
    // want. We start by seeking to the position we want to read within the
    // decompressed data.
    tempfile
        .seek(std::io::SeekFrom::Start(
            uncompressed_bytes_until_middle as u64,
        ))
        .unwrap();

    // Read some data, put it back into UTF8 and show it on the screen.
    {
        // 256 bytes should be enough to prove the point.
        let mut decompressed_content = [0; 256];
        let decompressed_bytes = tempfile.read(&mut decompressed_content).unwrap();
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

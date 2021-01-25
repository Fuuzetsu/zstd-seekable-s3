# zstd-seekable-s3 [![Actions Status: build](https://github.com/Fuuzetsu/zstd-seekable-s3/workflows/build/badge.svg)](https://github.com/Fuuzetsu/zstd-seekable-s3/actions?query=workflow%3A"build")

This is a wrapper over zstd_compressed and rusoto packages. It retrofits
S3 objects with seeking via the HTTP range header and implements a
decompression layer on top of anything we can read/seek. The result is
that we can read S3 objects that were compressed with (seekable) zstd
and only fetch the parts of the file we're interested as if we were
working on uncompressed data but without actually paying any bandwidth
or storage costs that come with storing uncompressed data.

See the `examples` directory for a potential way to use it.

This package is currently in experimental state, do expect the API to change.

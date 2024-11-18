# lnx-fs

An object-storage list abstraction over a file system.

This is optimized for writing and reading data _fast_ without relying on the file system cache.

The system is completely asynchronous using Direct IO and io_uring (sorry non-linux people!) backed
by the `glommio` runtime. 

It works by having immutable "tablets" where all writes are sequentially appended to the end of an active tablet,
once that tablet reaches a certain size, it is closed and a new tablet is created.
This allows the system to write very small blobs _very_ quickly which is good in lnx's use case where
some writes might only be one or two documents.

The downside to this approach is it uses a bit more disk space that strictly necessary, and relies on a periodic
GC job to compact tablets and cleanup dead blobs.

Ensuring durability of objects is largely down to the metastore, which is a SQLite database, files are only
registered in the metastore once the blob itself has been successfully flushed to disk.

## Buckets

Like S3, the system has the concept of buckets, they allow you to completely isolate two set of files or blobs which
can allow for mounting files on multiple disks, etc... 

The downside is too many buckets leads to the system not running efficiently because writes are too sparse across
the buckets. It is only recommended to have a couple buckets at most.

## Config Persistence

The system itself is self-contains, and each bucket has its own set of config values which can be set and unset
at runtime which will persist between restarts. When config values are unset they fall back to sane defaults.

## Example

```rust
use std::path::PathBuf;
use lnx_fs::{VirtualFileSystem, RuntimeOptions, FileSystemError, Body, Bytes};

#[tokio::main]
async fn main() -> Result<(), FileSystemError> {
    let options = RuntimeOptions::builder()
        .num_threads(2)
        .build();

    let mount_point = PathBuf::from_str("/tmp/my-fs/").unwrap();
    let service = VirtualFileSystem::mount(mount_point, options).await?;
    service.create_bucket("demo").await?;
    
    let bucket = service.bucket("demo").unwrap();
    
    let body = Body::complete(Bytes::from_static(b"Hello, World!"));
    bucket.write("example.txt", body.clone()).await?;
    
    let incoming = bucket.read("example.txt").await?;
    let data = incoming.collect().await?;
    assert_eq!(data, body);
}
```
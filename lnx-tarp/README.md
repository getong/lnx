# Tarp

A columnar format that specializes in total zero-copy access via Rkyv.

It is inspired by Parquet and has support for nested structures, but unlike Parquet is optimized for
random access. As part of this, it also does not have the same compression properties as Parquet; Integers
are packed to their nearest _complete primitive_ size (`i8`, `i16`, `i32`, `i64`) etc... Floats are left as-is
and strings are only dictionary compressed with Zstd.


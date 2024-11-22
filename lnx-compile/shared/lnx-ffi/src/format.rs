
#[repr(C)]
/// A signal that indicates what format a buffer is in.
pub enum Format {
    /// The buffer is in the JSON format as a single object.
    Json,
    /// The buffer is in the NDJSON format which contains potentially
    /// multiple documents.
    Ndjson,
    /// The buffer is in the MSGPACK format which contains potentially
    /// multiple documents _or_ a single object map.
    Msgpack,
}
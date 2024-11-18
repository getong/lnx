#[repr(u32)]
#[derive(Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
/// The data type of the column as stored on disk.
///
/// This type may be different to the type defined in the schema.
///
/// These types have no concept of nested structures.
pub enum StoredDataType {
    String = 1,
    I8 = 2,
    I16 = 3,
    I32 = 4,
    I64 = 5,
    F32 = 6,
    F64 = 7,
    Bytes = 8,
    Ipv6 = 9,
}

use crate::error::DocumentError;
use crate::format::Format;

#[macro_export]
/// A magic macro for exporting interactions between the
/// compiled library and main process.
/// 
/// This is done on the base implementation of a type implementing
/// [Document].
macro_rules! extern_magic {
    () => {};
}
/// A common document interface between the main lnx process and the compiled
/// libraries.
/// 
/// 
pub trait Document: Sized {
    /// The type the document uses as its [rkyv::Archived] view.
    type ArchivedView;
    
    /// Load a set of [Document] instances from the given buffer with the given format.
    fn load_owned_from_buffer(buffer: &[u8], kind: Format) -> Result<Vec<Self>, DocumentError>;

    /// Serializes the document to a rkyv buffer.
    fn serialize(&self) -> Result<(), DocumentError>;
    
    /// Gets a reference to the archived document.
    fn access<'a>(buffer: &'a [u8]) -> Result<&'a Self::ArchivedView, DocumentError>;
}

pub trait ArchivedDocument {
    /// Serialize the document to JSON string.
    fn serialize_to_json(&self);
    /// Serialize the document to MSGPACK binary data.
    fn serialize_to_msgpack(&self);
}
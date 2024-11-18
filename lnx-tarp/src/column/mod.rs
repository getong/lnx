mod primitive;

/// Accessor methods for a given column.
///
/// This provides a common abstraction over both owned and archived variants
/// of columns.
pub trait ColumnAccess<T> {
    /// Returns a slice reference to the values in the column.
    fn values(&self) -> &[T];
    /// Returns a slice of row IDs where the index of the values in this
    /// slice match the values returned by [Self::values].
    fn row_ids(&self) -> &[u16];
}

#[derive(Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
/// The inner column which is serialized by rkyv and can be accessed
/// via zero-copy.
pub(crate) struct InnerColumn<T> {
    /// The full qualified name of the column.
    name: Box<str>,
    /// The unique row ID within the row group.
    row_ids: Box<[u16]>,
    /// The values within the column.
    values: Box<[T]>,
}

//! Column access for primitive columns.
//!
//! This includes types that are cheaply clone and have no nested pointers i.e. `i8`, `Ipv6`, etc...

use super::{ArchivedInnerColumn, ColumnAccess, InnerColumn};

impl ColumnAccess<i8> for InnerColumn<i8> {
    #[inline]
    fn values(&self) -> &[i8] {
        self.values.as_ref()
    }

    #[inline]
    fn row_ids(&self) -> &[u16] {
        self.row_ids.as_ref()
    }
}

impl ColumnAccess<i8> for ArchivedInnerColumn<i8> {
    #[inline]
    fn values(&self) -> &[i8] {}

    #[inline]
    fn row_ids(&self) -> &[u16] {
        todo!()
    }
}

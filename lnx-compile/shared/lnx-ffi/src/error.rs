use std::ffi::CString;
use std::fmt::{Debug, Display, Formatter};

#[repr(C)]
#[derive(Debug, thiserror::Error)]
#[error("{kind:?}: {message}")]
/// An error that can occur from the document FFI api.
pub struct DocumentError {
    kind: ErrorKind,
    message: AssumedSafeCString,
}


#[repr(C)]
#[derive(Debug)]
/// The kind of error that originated.
pub enum ErrorKind {
    /// THe document is malformed.
    Malformed,
}


#[repr(transparent)]
/// A wrapper type that internally knows the CString 
/// is safely UTF-8. This is just for interop.
struct AssumedSafeCString(CString);

impl AssumedSafeCString {
    fn as_str(&self) -> &str {
        let inner = self.0.as_bytes();
        unsafe { std::str::from_utf8_unchecked(inner) }
    }
}

impl Display for AssumedSafeCString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        <str as Display>::fmt(self.as_str(), f)
    }
}

impl Debug for AssumedSafeCString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        <str as Debug>::fmt(self.as_str(), f)
    }
}

impl From<&str> for AssumedSafeCString {
    fn from(value: &str) -> Self {
        let inner = CString::new(value)
            .expect("String should not contain null terminator within it");
        Self(inner)
    }
}

impl From<String> for AssumedSafeCString {
    fn from(value: String) -> Self {
        let inner = CString::new(value)
            .expect("String should not contain null terminator within it");
        Self(inner)
    }
}
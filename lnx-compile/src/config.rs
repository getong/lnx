use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// Returns the path for the sccache binary. 
pub fn get_sccache_path() -> &'static Path {
    static PATH: OnceLock<PathBuf> = OnceLock::new();
    PATH.get_or_init(|| {
        let path = std::env::var("LNX_SCCACHE_PATH")
            .unwrap_or_else(|_| "sccache".to_string());
        PathBuf::from(path)
    })
}

/// Returns the build location for compiling schemas.
pub fn get_build_path() -> &'static Path {
    static PATH: OnceLock<PathBuf> = OnceLock::new();
    PATH.get_or_init(|| {
        let path = std::env::var("LNX_BUILD_PATH")
            .unwrap_or_else(|_| "/tmp/".to_string());
        PathBuf::from(path)
    })
}

/// Returns the path for compilation artefacts to be stored and cache.
/// 
/// This allows for faster compile times.
pub fn get_artefact_cache_path() -> &'static Path {
    static PATH: OnceLock<PathBuf> = OnceLock::new();
    PATH.get_or_init(|| {
        let path = std::env::var("LNX_ARTEFACT_CACHE_PATH")
            .unwrap_or_else(|_| "/tmp/".to_string());
        PathBuf::from(path)
    })
}

/// Returns the path to the `cargo` binary.
pub fn get_cargo_home() -> &'static Path {
    static PATH: OnceLock<PathBuf> = OnceLock::new();
    PATH.get_or_init(|| {
        let path = std::env::var("LNX_CARGO_PATH")
            .unwrap_or_else(|_| "cargo".to_string());
        PathBuf::from(path)
    })
}
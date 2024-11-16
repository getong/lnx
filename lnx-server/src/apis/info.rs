use poem_openapi::payload::Json;
use poem_openapi::OpenApi;

use super::Tag;

/// System information API endpoints
pub struct LnxInfoApi;

#[OpenApi(tag = Tag::InfoEndpoints)]
impl LnxInfoApi {
    #[oai(path = "/info/summary", method = "get")]
    /// Get Server Summary
    ///
    /// Returns summary information about the state of the system.
    async fn get_summary(&self) -> Json<bool> {
        Json(true)
    }
}

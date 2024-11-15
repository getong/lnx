use poem_openapi::OpenApi;
use poem_openapi::payload::Json;

use super::Tag;

/// Health check related API operations.
pub struct LnxHealthApi;

#[OpenApi(tag = Tag::HealthEndpoints)]
impl LnxHealthApi {
    #[oai(path = "/health/check", method = "get")]
    /// Check Health
    ///
    /// A health check endpoint to check the API is available.
    async fn get_health_check(&self) -> Json<bool> {
        Json(true)
    }
}


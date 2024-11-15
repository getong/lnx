use poem_openapi::OpenApi;
use poem_openapi::payload::Json;

use super::Tag;

/// System information API endpoints
pub struct LnxQueryApi;

#[OpenApi(tag = Tag::QueryEndpoints)]
impl LnxQueryApi {
    #[oai(path = "/query/fetchall", method = "post")]
    /// Execute Fetch All Query
    ///
    /// Executes the provided SQL query and returns all documents matching
    /// the query with a **default limit of `1000` records** if no limit is explicitly
    /// provided in the query.
    async fn fetchall(&self) -> Json<bool> {
        Json(true)
    }
    
    #[oai(path = "/query/fetchone", method = "post")]
    /// Execute Fetch One Query
    ///
    /// Executes the provided SQL query and returns one document matching
    /// the query.
    /// 
    /// If no `ORDER BY` clause is provided, the system will use the _first matching_ document
    /// which may change between calls as data is updated or indexes compacted.
    async fn fetchone(&self) -> Json<bool> {
        Json(true)
    }

    #[oai(path = "/query/explain", method = "post")]
    /// Explain Query
    ///
    /// Generates the query plan for the specified SQL query.
    /// 
    /// This allows you to inspect how it will match and process documents.
    async fn explain(&self) -> Json<bool> {
        Json(true)
    }
}


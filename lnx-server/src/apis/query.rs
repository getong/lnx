use lnx_query::sql;
use poem_openapi::payload::Json;
use poem_openapi::{Object, OpenApi};

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
    async fn fetchall(
        &self,
        Json(payload): Json<QueryPayload>,
    ) -> poem::Result<Json<bool>> {
        dbg!(payload);
        Ok(Json(true))
    }

    #[oai(path = "/query/fetchone", method = "post")]
    /// Execute Fetch One Query
    ///
    /// Executes the provided SQL query and returns one document matching
    /// the query.
    async fn fetchone(&self, Json(payload): Json<QueryPayload>) -> Json<bool> {
        dbg!(payload);
        Json(true)
    }

    #[oai(path = "/query/explain", method = "post")]
    /// Explain Query
    ///
    /// Generates the query plan for the specified SQL query.
    async fn explain(&self, Json(payload): Json<QueryPayload>) -> Json<bool> {
        dbg!(payload);
        Json(true)
    }
}

#[derive(Debug, Object)]
/// The query payload to execute.
struct QueryPayload {
    /// The SQL query string.
    query: sql::SqlStatements,
    #[oai(default)]
    /// The parameter values to inject into the query.
    parameters: Vec<serde_json::Value>,
}

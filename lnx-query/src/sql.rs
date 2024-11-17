//! lnx SQL parsing types.
//!
//! These types provide pre-defined parsers which can then be converted to
//! Tantivy queries.

use std::borrow::Cow;

use poem_openapi::registry::{MetaSchema, MetaSchemaRef};
use poem_openapi::types::{ParseError, ParseResult};
use serde_json::{json, Value};
use sqlparser::ast::Statement;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tracing::error;

#[derive(Debug, Clone)]
pub struct SqlStatements(pub Vec<Statement>);

impl poem_openapi::types::Type for SqlStatements {
    const IS_REQUIRED: bool = false;
    type RawValueType = Self;
    type RawElementValueType = Self;

    fn name() -> Cow<'static, str> {
        Cow::Borrowed("SqlStatements")
    }

    fn schema_ref() -> MetaSchemaRef {
        MetaSchemaRef::Inline(Box::new(MetaSchema {
            rust_typename: Some("SqlStatements"),
            ty: "SqlStatements",
            format: None,
            title: None,
            description: None,
            external_docs: None,
            default: None,
            required: vec![],
            properties: vec![],
            items: None,
            additional_properties: None,
            enum_items: vec![],
            deprecated: false,
            any_of: vec![],
            one_of: vec![],
            all_of: vec![],
            discriminator: None,
            read_only: false,
            write_only: false,
            example: Some(json!(
                "INSERT INTO books (title, description) VALUES ($1, $2);"
            )),
            multiple_of: None,
            maximum: None,
            exclusive_maximum: None,
            minimum: None,
            exclusive_minimum: None,
            max_length: None,
            min_length: None,
            pattern: None,
            max_items: None,
            min_items: None,
            unique_items: None,
            max_properties: None,
            min_properties: None,
        }))
    }

    fn as_raw_value(&self) -> Option<&Self::RawValueType> {
        Some(self)
    }

    fn raw_element_iter<'a>(
        &'a self,
    ) -> Box<dyn Iterator<Item = &'a Self::RawElementValueType> + 'a> {
        Box::new(std::iter::empty())
    }
}

impl poem_openapi::types::ParseFromJSON for SqlStatements {
    fn parse_from_json(value: Option<Value>) -> ParseResult<Self> {
        let Value::String(value) = value.unwrap_or(Value::Null) else {
            return Err(ParseError::expected_type(Value::String(String::new())));
        };
        let statements =
            Parser::parse_sql(&PostgreSqlDialect {}, &value).map_err(|e| {
                error!(error = ?e, "Failed to parse query");
                format!("Failed to parse query: {e}")
            })?;
        Ok(Self(statements))
    }
}

impl poem_openapi::types::ToJSON for SqlStatements {
    fn to_json(&self) -> Option<Value> {
        Some(json!("Stmts"))
    }
}

#[cfg(test)]
mod tests {
    use poem_openapi::types::ParseFromJSON;

    use super::*;

    #[rstest::rstest]
    #[case(json!("SELECT * FROM foo WHERE foo = 'bar' AND example = $1;"), true)]
    #[case(json!("INSERT INTO foobar (col1, col2) VALUES (true, $1)"), true)]
    #[case(json!("DELETE FROM barfoo WHERE col1 = $1 AND col2 = $2;"), true)]
    #[case(json!("CREATE TABLE foobar (col1 TEXT, col2 BIGINT);"), true)]
    #[case(json!("CREATE TABLE foobar (col1 TEXT, col2 BIGINT) WITH (tokenizers = ( example = 'raw' ));"), true)]
    #[case(json!("Not a query"), false)]
    #[case(
        json!(r#"
            SELECT foo, bar FROM books;
            DELETE FROM books WHERE id = $1;
        "#), 
        true
    )]
    fn test_sql_statements_parse(#[case] query: Value, #[case] is_ok: bool) {
        let parsed_result = SqlStatements::parse_from_json(Some(query));
        assert_eq!(
            parsed_result.is_ok(),
            is_ok,
            "Expected parse status ok={is_ok}, got: {parsed_result:?}"
        );
    }
}

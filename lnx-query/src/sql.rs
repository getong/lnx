//! lnx SQL parsing types.
//!
//! These types provide pre-defined parsers which can then be converted to
//! Tantivy queries.

use std::borrow::Cow;

use serde_json::{json, Value};
use poem_openapi::registry::{MetaSchema, MetaSchemaRef};
use poem_openapi::types::{ParseError, ParseResult};
use sqlparser::ast::{Query, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;
use tracing::error;

#[derive(Debug, Clone)]
/// A SQL `SELECT` query parser type.
///
/// This expects an input string that is in the format:
///
/// ```sql
/// SELECT column1, column2, ... FROM table_name WHERE conditions;
/// ```
///
/// `JOIN`s and CTEs are currently not supported.
///
pub struct SqlSelectQuery(pub Box<Query>);

impl poem_openapi::types::Type for SqlSelectQuery {
    const IS_REQUIRED: bool = false;
    type RawValueType = Self;
    type RawElementValueType = Self;

    fn name() -> Cow<'static, str> {
        Cow::Borrowed("SqlSelectQuery")
    }

    fn schema_ref() -> MetaSchemaRef {
        MetaSchemaRef::Inline(Box::new(MetaSchema {
            rust_typename: Some("SqlSelectQuery"),
            ty: "SqlSelectQuery",
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
            example: Some(json!("SELECT id, name FROM customers WHERE (fts(name, $1) OR fuzzy(description, $2)) AND age > $3;")),
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

    fn raw_element_iter<'a>(&'a self) -> Box<dyn Iterator<Item=&'a Self::RawElementValueType> + 'a> {
        Box::new(std::iter::empty())
    }
}

impl poem_openapi::types::ParseFromJSON for SqlSelectQuery {
    fn parse_from_json(value: Option<Value>) -> ParseResult<Self> {
        let Value::String(value) = value.unwrap_or(Value::Null) else { 
            return Err(ParseError::expected_type(Value::String(String::new())))
        };

        let mut tokenizer = Tokenizer::new(&PostgreSqlDialect{}, &value);
        let tokens = tokenizer
            .tokenize()
            .map_err(|e| ParseError::custom(format!("Failed to tokenize query: {e}")))?;

        let mut parser = Parser::new(&PostgreSqlDialect {})
            .with_tokens(tokens);
        let statement = parser
            .parse_query()
            .map_err(|e| ParseError::custom(format!("Failed to parse query: {e}")))?;

        Ok(Self(statement))
    }
}

impl poem_openapi::types::ToJSON for SqlSelectQuery {
    fn to_json(&self) -> Option<Value> {
        Some(json!(self.0.to_string()))
    }
}


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
            example: Some(json!("INSERT INTO books (title, description) VALUES ($1, $2);")),
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

    fn raw_element_iter<'a>(&'a self) -> Box<dyn Iterator<Item=&'a Self::RawElementValueType> + 'a> {
        Box::new(std::iter::empty())
    }
}

impl poem_openapi::types::ParseFromJSON for SqlStatements {
    fn parse_from_json(value: Option<Value>) -> ParseResult<Self> {
        let Value::String(value) = value.unwrap_or(Value::Null) else {
            return Err(ParseError::expected_type(Value::String(String::new())))
        };
        let statements = Parser::parse_sql(&PostgreSqlDialect{}, &value)
            .map_err(|e| { 
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
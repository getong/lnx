use poem_openapi::Tags;

mod health;
mod info;
mod query;

pub use self::health::LnxHealthApi;
pub use self::info::LnxInfoApi;
pub use self::query::LnxQueryApi;


#[derive(Tags)]
pub(super) enum Tag {
    #[oai(rename = "Health Endpoints")]
    /// Service health related endpoints
    ///
    /// This can be used when behind load balancers or to ensure an instance is operating
    /// correctly.
    ///
    /// The basic health check endpoint will always return `200 OK` and can be used to
    /// check the service is reachable.
    HealthEndpoints,
    #[oai(rename = "Info Endpoints")]
    /// Service information endpoints allows access to inspect:
    ///
    /// - Service Info
    ///     * Version
    ///     * Listen address
    ///     * Number of indexes
    ///     * Documents stored
    ///     * Uptime
    /// - Host machine Info
    ///     * OS & Version
    ///     * System limits
    ///     * Disk space
    ///     * CPU usage/allowance
    ///     * Memory usage/allowance
    ///
    InfoEndpoints,
    #[oai(rename = "Query Endpoints")]
    /// Used for executing queries against the database, lnx uses a SQL dialect to query documents.
    /// Queries can be parameterized to prevent injection attacks and work similarly to
    /// how you would interact with PostgreSQL or SQLite.
    /// 
    /// See the [SQL Syntax Documentation](#tag/SQL-Syntax) for more.
    ///
    QueryEndpoints,
    #[allow(unused)]    // Used in OpenAPI docs.
    #[oai(rename = "SQL Syntax")]
    /// Welcome to the Introduction page for the SQL Syntax in lnx! 
    /// 
    /// In this section, we'll provide a brief overview of the SQL syntax supported by lnx. 
    /// You can find more in-depth examples and details by browsing the other pages in this category.
    /// 
    /// lnx itself uses [GlueSQL](https://github.com/gluesql/gluesql) for its SQL parsing and 
    /// interfacing, although it does not support all features GlueSQL supports.
    /// 
    /// Internally lnx acts a bit like Cassandra, where `INSERT` operations are _always_ 
    /// `UPSERT` (Insert or Update) due to consistency behaviours within lnx.
    /// 
    /// **NOTE: Currently JOINs and CTEs are not supported.**
    /// 
    /// Here's a list of some basic SQL statements you can use with GlueSQL:
    /// 
    /// ## Basic Operations Syntax
    ///
    /// ### Creating Tables
    /// 
    /// Tables in lnx are where all your data is kept, you can create them using a family typical
    /// SQL query:
    ///
    /// ```sql
    /// CREATE TABLE [IF NOT EXISTS] table_name (column_name1 data_type1, column_name2 data_type2, ...);
    /// ```
    ///
    /// See the list of [supported datatypes](#section/Supported-Datatypes) for what column types
    /// are available.
    ///  
    /// #### Columns Defaults
    /// 
    /// Columns can be specified with a `DEFAULT <value>` option to make a column non-required
    /// when inserting data into the table.
    /// 
    /// #### Nullable Columns
    /// 
    /// By default, columns are always nullable unless it is a `PRIMARY KEY` field in order
    /// to be consistent with the SQL syntax. 
    /// Fields can be declared as `NOT NULL` to strictly forbid a field value being `null`.
    /// 
    /// Columns that allow `null` are automatically optional and default to `null` if missing
    /// during insertion time.
    /// 
    /// ### Inserting Data
    ///
    /// ```sql
    /// INSERT INTO table_name (column1, column2, ...) VALUES (value1, value2, ...);
    /// ```
    ///
    /// #### Required Columns
    /// 
    /// Only columns that are `NOT NULL` and have no default value specified _must_ be provided,
    /// In the event a column is missing, its default value will be set.
    /// 
    /// ### Selecting Data
    ///
    /// ```sql
    /// SELECT column1, column2, ... FROM table_name WHERE conditions;
    /// ```
    ///
    /// #### Tip
    /// 
    /// Select what you need only, avoid using `*` for selecting columns as this increases
    /// the amount of data the system needs to retrieve.
    /// 
    /// ### Updating Data
    ///
    /// ```sql
    /// UPDATE table_name SET column1 = value1, column2 = value2, ... WHERE conditions;
    /// ```
    ///
    /// ### Deleting Data
    ///
    /// ```sql
    /// DELETE FROM table_name WHERE conditions;
    /// ```
    /// 
    /// ## Supported Datatypes
    /// 
    /// lnx provides several core datatypes alongside a set of datatypes that are aliases
    /// for convenience.
    /// 
    /// | SQL Name  | Description                              | Size On Disk |
    /// | --------- | ---------------------------------------- | ------------ |
    /// | string    | Alias of type `string`                   | variable     |
    /// | text      | A variably sized UTF-8 encoded string    | variable     |
    /// | bigint    | Alias of type `int64`                    | 8 Bytes      |
    /// | integer   | Alias of type `int32`                    | 4 Bytes      |
    /// | int64     | A signed 64-bit integer                  | 8 Bytes      |
    /// | int32     | A signed 32-bit integer                  | 4 Bytes      |
    /// | uint64    | An unsigned 64-bit integer               | 8 Bytes      |
    /// | uint32    | An unsigned 32-bit integer               | 4 Bytes      |
    /// | float     | Alias of type `float32`                  | 4 Bytes      |
    /// | double    | Alias of type `float64`                  | 8 Bytes      |
    /// | float64   | A 64-bit double-precision float          | 8 Bytes      |
    /// | float32   | A 32-bit single-precision float          | 4 Bytes      |
    /// | bytea     | Alias of `bytes`                         | variable     |
    /// | bytes     | A variably sized byte array              | variable     |
    /// | ip        | A Ipv4 or Ipv6 address                   | 16 Bytes     |
    /// | facet     | A hierarchal facet string                | variable     |
    /// | timestamp | Alias of type `datetime`                 | 8 Bytes      |
    /// | datetime  | A timestamp starting from the UNIX EPOCH | 8 Bytes      |
    /// | date      | A date value with no time attached       | 4 Bytes      |
    /// | boolean   | Alias of type `bool`                     | 1 Bytes      |
    /// | bool      | A `true`/`false` value                   | 1 Bytes      |
    /// 
    /// ### Arrays
    /// 
    /// Other than the core datatypes, lnx supports arrays which can be 
    /// declared as `[]dtype` when creating a table.
    ///
    /// ### Maps
    ///
    /// TODO: Add map docs
    /// 
    /// ### Nested Objects
    /// 
    /// TODO: Add nested objects docs
    /// 
    /// ## Supported Functions
    /// 
    /// TODO: Add supported functions docs
    /// 
    SQLSyntax,
}
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
    /// This allows you to inspect how it will match and process documents.
    ///
    /// When creating a query, it is _always_ recommended to pass values via
    /// parameterization rather than hard coded into the SQL.
    ///
    /// Parameters can be specified as `$n` (Postgres Syntax), for example, instead of:
    ///
    /// ```json
    /// {
    ///     "query": "SELECT id, name FROM customers WHERE id = 1234;" 
    /// }
    /// ```
    ///
    /// Do:
    ///
    /// ```json
    /// {
    ///     "query": "SELECT id, name FROM customers WHERE id = $1;", 
    ///     "parameters": [1234], 
    /// } 
    /// ```
    ///
    /// This prevents SQL injection attacks and can even improve query performance as
    /// this system is able to cache different parts of the query.
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
    /// 
    /// ### Updating Data
    ///
    /// ```sql
    /// UPDATE table_name SET column1 = value1, column2 = value2, ... WHERE conditions;
    /// ```
    ///
    /// 
    /// ### Deleting Data
    ///
    /// ```sql
    /// DELETE FROM table_name WHERE conditions;
    /// ```
    /// 
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
    /// 
    /// ### Maps
    ///
    /// TODO: Add map docs
    /// 
    /// 
    /// ### Nested Objects
    /// 
    /// TODO: Add nested objects docs
    /// 
    /// 
    /// ## Supported Functions
    /// 
    /// ### Search Queries
    /// 
    /// lnx is first and foremost; a search engine, so naturally it has several functions
    /// for executing queries with various behaviours:
    /// 
    /// 
    /// #### Full text search query - `fts(col, query) -> float32`
    /// 
    /// Computes the BM25 score of the query string against the column value, this 
    /// will tokenize the input `query` text with the same tokenizer as is specified for
    /// the given column.
    /// 
    /// Terms which have the same prefix of a query term can be matched by adding a `*` (asterisk)
    /// to the end of the term. For example `'foo'` will only match `"foo"` and ignore `"foobar"`.
    /// But `'foo*'` will match _both_ `"foo"` and `"foobar"`
    /// 
    /// Returns the `float32` score of the query.
    /// 
    /// ```sql
    /// SELECT *, score() as score FROM customers WHERE fts(name, 'Tim* Micheal') > 0.2 ORDER BY score DESC LIMIT 10;
    /// ```
    /// 
    /// 
    /// #### Fuzzy search query - `fuzzy(col, query) -> float32`
    /// 
    /// Computes the BM25 score of query string against the column value with fuzzy
    /// matching enabled, this allows for the query to be tolerant of spelling mistakes
    /// in queries.
    ///
    /// Scoring of the match is adjusted based on the edit distance of the match, i.e. how
    /// many characters the engine had to change or remove in order to "match" the value.
    /// Matches with a smaller edit distance will have a higher percentage of the original
    /// BM25 score, the further the distance, the lower the percentage of the original
    /// BM25 score is used.
    /// 
    /// For example, a query of `'John Grishme'` will have an edit distance of `2`
    /// when matched against the value `"John Grisham"`, if its BM25 score is `0.8`
    /// and the configured percentage for an edit distance of `2` is `60%` the final
    /// score of the match will be `0.48` .
    /// 
    /// Returns the `float32` score of the query.
    ///
    /// ```sql
    /// SELECT *, score() as score FROM customers WHERE fuzzy(name, 'John Grishme') > 0.2 ORDER BY score DESC LIMIT 10;
    /// ```
    ///
    ///
    /// #### Levenshtein (Fuzzy) search query - `levenshtein(col, query) -> float32`
    ///
    /// Performs a fuzzy match on the column values using the input query.
    /// 
    /// Terms are matched using Levenshtein distance and **do not return a BM25 score**.
    /// 
    /// It is generally not recommended to use this query function for user facing search
    /// as its matches are hard to sort and score. Instead `fastfuzzy` should be preferred.
    /// 
    /// There are still correct use cases however, for example, doing fuzzy matching on
    /// large datasets where you only care if it _can_ match and not about how relevant 
    /// they are.
    ///
    /// ```sql
    /// SELECT * FROM customers WHERE levenshtein(name, 'John Grishme') LIMIT 10;
    /// ```
    /// 
    /// Returns the `float32` **either** `1.0` (match) or `0.0` (no match).
    /// 
    /// 
    /// #### Regex search query - `regex(col, query) -> float32`
    ///
    /// Matches terms in the given `col` which match the given regex pattern.
    /// 
    /// **NOTE: Your query will _not_ be tokenized but your column values _will_ be, 
    /// which may cause confusing behaviour if attempting to match an entire sentence 
    /// instead of a single term.**
    ///
    /// ```sql
    /// SELECT * FROM customers WHERE regex(name, 'bob[0-9]+') LIMIT 10;
    /// ```
    /// 
    /// Returns the `float32` **either** `1.0` (match) or `0.0` (no match).
    SQLSyntax,
}
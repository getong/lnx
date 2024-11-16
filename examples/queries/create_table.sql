CREATE TABLE IF NOT EXISTS books (
    id bigint primary key,
    title text not null,
    description text not null default '',
    category text[]
);
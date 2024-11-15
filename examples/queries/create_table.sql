CREATE TABLE IF NOT EXISTS books (
    id bigint primary key,
    title text not null indexed,
    description text not null indexed default '',
    category []text
);
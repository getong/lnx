SELECT
    id,
    title,
    description,
    rating,
    category,
    score() as score
FROM books
WHERE
    fuzzy(title, $1)
    AND fuzzy(description, $1)
    AND category = $2
ORDER BY score DESC
LIMIT 10;
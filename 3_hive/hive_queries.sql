/*
1.) Top N-words by subject (subjects = cs, math, physics, q-bio, q-fin, stat)
2.) N-most active authors
3.) Average number of authors per paper
4.) Author's 10 most common words
5.) TFIDF

==================================
1. Top N-words by subject (subjects = cs, math, physics, q-bio, q-fin, stat) */

SELECT word, count(word) as cnt
FROM (
    SELECT url_id, subject
    FROM paper_meta
    WHERE subject = 'q-fin'
    ) as LHS
JOIN
    (SELECT url_id, word
    FROM words
    ) as RHS
WHERE LHS.url_id = RHS.url_id
GROUP BY word
ORDER BY cnt desc
LIMIT 20;

/*---------
2. N-most active authors*/

SELECT author, count(author) as cnt
FROM authors
GROUP BY author
ORDER BY cnt DESC
LIMIT 10;

/*---------
3. Average number of authors per paper*/

SELECT avg(size(author))
FROM authors_tmp;

/*--------
4. Author's words used more than 10 times overall*/

SELECT author, word, sum(cnt) as cnt
FROM
    (SELECT author, word, count(word) over (PARTITION BY author, word) as cnt
    FROM
        ( SELECT LHS.url_id, author, word
        FROM
            (
            SELECT url_id, author
            FROM authors
            ) as LHS
        JOIN
            (SELECT url_id, word
            FROM words
            ) as RHS
        WHERE LHS.url_id = RHS.url_id
        ) as inner_q
    ) as outer_q
WHERE cnt > 10
GROUP BY author, word
ORDER BY cnt
LIMIT 20;

/*--------
5. TF-IDF

---- remember to change total N for number of papers in numerator -----*/

select count(*) from words_tmp;
/*-- 1256826*/

SELECT distinct LHS.word, tf * log(2, 1256826 / df) as tfidf
FROM
    (SELECT url_id, word, count(word) as tf
    FROM words
    GROUP BY url_id, word) as LHS
JOIN
    (SELECT word, count(word) as df
    FROM words
    GROUP BY word) as RHS
ON LHS.word = RHS.word
ORDER by tfidf desc
LIMIT 20;

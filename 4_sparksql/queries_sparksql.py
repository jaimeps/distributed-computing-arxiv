# Students: D. Wen, A. Romriell, J. Pastor, J. Pollard
# MSAN 694 Project



''' QUERIES ================================================================'''
class queries_obj(object):
    def __init__(self):
        pass

    def num_words(self, subject):
        query_string = '''SELECT words, count(words) as cnt
                FROM (
                    SELECT url, subject
                    FROM meta
                    WHERE subject = "''' + subject +'''"
                    ) as LHS
                JOIN
                    words2 as RHS
                ON LHS.url = RHS.url
                GROUP BY words
                ORDER BY cnt desc
                LIMIT 20'''
        return query_string

    def most_active_authors(self):
        query_string = '''SELECT authors, count(authors) as cnt
                    FROM
                        authors2 as Q1
                    GROUP BY authors
                    ORDER BY cnt DESC
                    LIMIT 10'''
        return query_string

    def avg_authors(self):
        query_string = '''SELECT avg(size(authors)) FROM authors'''
        return query_string

    def author_words_10(self):
        query_string = '''SELECT authors, words, cnt FROM
            (SELECT authors, words, count(*) as cnt
            FROM
                (SELECT LHS.url, authors, words
                FROM authors2 as LHS
                JOIN words2 as RHS
                ON LHS.url = RHS.url) as inner_q
            GROUP BY authors, words
            ) as outer_q
        where cnt > 10
        ORDER BY cnt DESC
        LIMIT 20'''
        return query_string


    def tf_idf(self):
        query_string = '''SELECT distinct LHS.words, tf * log(2.0, 1256826 / cast(df as float)) as tfidf
            FROM
                (SELECT url, words, count(words) as tf
                FROM words2 as LQ1
                GROUP BY url, words) as LHS
            JOIN
                (SELECT words, count(words) as df
                FROM words2 as RQ1
                GROUP BY words) as RHS
            ON LHS.words = RHS.words
            ORDER by tfidf
            LIMIT 20'''
        return query_string

